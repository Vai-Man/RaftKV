#include <algorithm>

#include "raftkv/raftkv.h"

namespace raftkv {

// ---------------------------------------------------------------- KvStore
Result KvStore::apply(const Command& cmd) {
    // Exactly-once for retried client commands: a repeated (client, seq) returns the first result.
    if (cmd.client_id != 0) {
        auto it = last_by_client_.find(cmd.client_id);
        if (it != last_by_client_.end() && cmd.seq <= it->second.first) return it->second.second;
    }
    Result r;
    switch (cmd.type) {
        case Command::Type::Put:
            data_[cmd.key] = cmd.value;
            break;
        case Command::Type::Delete:
            r.ok = data_.erase(cmd.key) > 0;
            break;
        case Command::Type::Get: {
            auto it = data_.find(cmd.key);
            if (it == data_.end()) r.ok = false;
            else r.value = it->second;
            break;
        }
        case Command::Type::Noop:
            break;
    }
    if (cmd.client_id != 0) last_by_client_[cmd.client_id] = {cmd.seq, r};
    return r;
}

std::optional<std::string> KvStore::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
}

void KvStore::clear() {
    data_.clear();
    last_by_client_.clear();
}

// ---------------------------------------------------------------- Node
Node::Node(NodeId id, int cluster_size, Config cfg, std::uint32_t seed)
    : id_(id), n_(cluster_size), cfg_(cfg), rng_(seed + 7919u * static_cast<std::uint32_t>(id)) {
    log_.push_back(LogEntry{});  // sentinel at index 0
    next_index_.assign(n_, 1);
    match_index_.assign(n_, 0);
    reset_election_deadline(0);
}

void Node::reset_election_deadline(Time now) {
    std::uniform_int_distribution<int> d(cfg_.election_timeout_min, cfg_.election_timeout_max);
    election_deadline_ = now + static_cast<Time>(d(rng_));
}

void Node::become_follower(Term term) {
    if (term > term_) {
        term_ = term;
        voted_for_ = -1;
    }
    role_ = Role::Follower;
    votes_ = 0;
}

void Node::crash() {
    role_ = Role::Follower;
    leader_id_ = -1;
    commit_index_ = 0;
    last_applied_ = 0;
    votes_ = 0;
    kv_.clear();
    results_.clear();
    became_leader_ = false;
    std::fill(next_index_.begin(), next_index_.end(), 1);
    std::fill(match_index_.begin(), match_index_.end(), 0);
}

void Node::restart(Time now) { reset_election_deadline(now); }

void Node::tick(Time now, Outbox& out) {
    if (role_ == Role::Leader) {
        if (now - last_heartbeat_ >= static_cast<Time>(cfg_.heartbeat_interval)) {
            last_heartbeat_ = now;
            broadcast_append(out);
        }
    } else if (now >= election_deadline_) {
        start_election(now, out);
    }
}

void Node::start_election(Time now, Outbox& out) {
    ++term_;
    role_ = Role::Candidate;
    voted_for_ = id_;
    votes_ = 1;
    leader_id_ = -1;
    reset_election_deadline(now);
    if (n_ == 1) {  // degenerate single-node cluster
        become_leader(now, out);
        return;
    }
    for (NodeId p = 0; p < n_; ++p) {
        if (p == id_) continue;
        Message m;
        m.type = MsgType::RequestVote;
        m.from = id_;
        m.to = p;
        m.term = term_;
        m.last_log_index = last_log_index();
        m.last_log_term = last_log_term();
        out.push_back(std::move(m));
    }
}

void Node::become_leader(Time now, Outbox& out) {
    role_ = Role::Leader;
    leader_id_ = id_;
    became_leader_ = true;
    std::fill(next_index_.begin(), next_index_.end(), last_log_index() + 1);
    std::fill(match_index_.begin(), match_index_.end(), 0);
    // A no-op in the new term lets the leader commit entries from earlier terms (Raft section 5.4.2).
    LogEntry noop;
    noop.term = term_;
    log_.push_back(noop);
    last_heartbeat_ = now;
    if (n_ == 1) advance_commit(), apply_committed();
    broadcast_append(out);
}

bool Node::submit(const Command& cmd, Index& index, Term& term) {
    if (role_ != Role::Leader) return false;
    LogEntry e;
    e.term = term_;
    e.cmd = cmd;
    log_.push_back(std::move(e));
    index = last_log_index();
    term = term_;
    if (n_ == 1) advance_commit(), apply_committed();
    return true;
}

void Node::broadcast_append(Outbox& out) {
    for (NodeId p = 0; p < n_; ++p)
        if (p != id_) send_append(p, out);
}

void Node::send_append(NodeId peer, Outbox& out) {
    Message m;
    m.type = MsgType::AppendEntries;
    m.from = id_;
    m.to = peer;
    m.term = term_;
    m.prev_log_index = next_index_[peer] - 1;
    m.prev_log_term = log_[m.prev_log_index].term;
    m.leader_commit = commit_index_;
    const Index last = last_log_index();
    for (Index i = next_index_[peer]; i <= last && m.entries.size() < cfg_.max_entries_per_message; ++i)
        m.entries.push_back(log_[i]);
    out.push_back(std::move(m));
}

void Node::receive(const Message& m, Time now, Outbox& out) {
    // Rule for every message: a higher term demotes us to follower.
    if (m.term > term_) {
        become_follower(m.term);
        leader_id_ = -1;
    }
    switch (m.type) {
        case MsgType::RequestVote: on_request_vote(m, now, out); break;
        case MsgType::RequestVoteReply: on_request_vote_reply(m, now, out); break;
        case MsgType::AppendEntries: on_append_entries(m, now, out); break;
        case MsgType::AppendEntriesReply: on_append_entries_reply(m, out); break;
    }
}

void Node::on_request_vote(const Message& m, Time now, Outbox& out) {
    Message r;
    r.type = MsgType::RequestVoteReply;
    r.from = id_;
    r.to = m.from;
    r.term = term_;
    r.vote_granted = false;
    if (m.term == term_ && (voted_for_ == -1 || voted_for_ == m.from)) {
        const bool up_to_date = m.last_log_term > last_log_term() ||
                                (m.last_log_term == last_log_term() && m.last_log_index >= last_log_index());
        if (up_to_date) {
            voted_for_ = m.from;
            r.vote_granted = true;
            reset_election_deadline(now);
        }
    }
    out.push_back(std::move(r));
}

void Node::on_request_vote_reply(const Message& m, Time now, Outbox& out) {
    if (role_ != Role::Candidate || m.term != term_ || !m.vote_granted) return;
    if (++votes_ > n_ / 2) become_leader(now, out);
}

void Node::on_append_entries(const Message& m, Time now, Outbox& out) {
    Message r;
    r.type = MsgType::AppendEntriesReply;
    r.from = id_;
    r.to = m.from;
    r.term = term_;
    r.success = false;

    if (m.term < term_) {  // stale leader
        out.push_back(std::move(r));
        return;
    }
    // Same term: this sender is the legitimate leader.
    role_ = Role::Follower;
    leader_id_ = m.from;
    reset_election_deadline(now);

    if (m.prev_log_index > last_log_index() || log_[m.prev_log_index].term != m.prev_log_term) {
        r.hint_index = std::min(m.prev_log_index, last_log_index() + 1) - 1;
        out.push_back(std::move(r));
        return;
    }

    Index idx = m.prev_log_index;
    for (const LogEntry& e : m.entries) {
        ++idx;
        if (idx <= last_log_index()) {
            if (log_[idx].term == e.term) continue;  // already have it
            log_.resize(idx);                         // conflict: drop this entry and everything after
        }
        log_.push_back(e);
    }
    const Index last_new = m.prev_log_index + m.entries.size();
    if (m.leader_commit > commit_index_) {
        commit_index_ = std::min(m.leader_commit, last_new);
        apply_committed();
    }
    r.success = true;
    r.match_index = last_new;
    out.push_back(std::move(r));
}

void Node::on_append_entries_reply(const Message& m, Outbox& out) {
    if (role_ != Role::Leader || m.term != term_) return;
    if (m.success) {
        match_index_[m.from] = std::max(match_index_[m.from], m.match_index);
        next_index_[m.from] = match_index_[m.from] + 1;
        advance_commit();
        apply_committed();
    } else {
        // Back up using the follower's hint, but always make progress.
        Index next = std::min<Index>(next_index_[m.from] - 1, m.hint_index + 1);
        next_index_[m.from] = std::max<Index>(1, next);
        send_append(m.from, out);
    }
}

void Node::advance_commit() {
    for (Index n = last_log_index(); n > commit_index_; --n) {
        if (log_[n].term != term_) break;  // only entries from the current term commit by counting
        int replicas = 1;                  // the leader itself
        for (NodeId p = 0; p < n_; ++p)
            if (p != id_ && match_index_[p] >= n) ++replicas;
        if (replicas > n_ / 2) {
            commit_index_ = n;
            break;
        }
    }
}

void Node::apply_committed() {
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        results_[last_applied_] = kv_.apply(log_[last_applied_].cmd);
    }
}

}  // namespace raftkv
