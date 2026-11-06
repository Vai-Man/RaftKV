#include <algorithm>
#include <sstream>

#include "raftkv/raftkv.h"

namespace raftkv {

Cluster::Cluster(int size, std::uint32_t seed, Config cfg, NetworkConfig net)
    : alive_(size, true), group_(size, 0), rng_(seed ^ 0x9e3779b9u), net_(net) {
    nodes_.reserve(size);
    for (int i = 0; i < size; ++i) nodes_.emplace_back(i, size, cfg, seed);
    verified_.assign(size, 0);
}

bool Cluster::reachable(NodeId a, NodeId b) const { return group_[a] == group_[b]; }

void Cluster::send(Message m) {
    if (!reachable(m.from, m.to)) return;
    if (net_.drop_rate > 0.0) {
        std::uniform_real_distribution<double> u(0.0, 1.0);
        if (u(rng_) < net_.drop_rate) return;
    }
    std::uniform_int_distribution<int> delay(net_.min_delay, net_.max_delay);
    inflight_.push(Pending{now_ + static_cast<Time>(delay(rng_)), msg_seq_++, std::move(m)});
}

void Cluster::deliver_due() {
    while (!inflight_.empty() && inflight_.top().at <= now_) {
        Message m = inflight_.top().msg;
        inflight_.pop();
        if (!alive_[m.to] || !reachable(m.from, m.to)) continue;
        Outbox out;
        nodes_[m.to].receive(m, now_, out);
        for (auto& o : out) send(std::move(o));
    }
}

void Cluster::step() {
    ++now_;
    deliver_due();
    for (int i = 0; i < size(); ++i) {
        if (!alive_[i]) continue;
        Outbox out;
        nodes_[i].tick(now_, out);
        for (auto& o : out) send(std::move(o));
    }
    check_invariants();
}

void Cluster::run(int ticks) {
    for (int i = 0; i < ticks; ++i) step();
}

void Cluster::crash(NodeId id) {
    alive_[id] = false;
    nodes_[id].crash();
    verified_[id] = 0;
}

void Cluster::restart(NodeId id) {
    if (alive_[id]) return;
    alive_[id] = true;
    nodes_[id].restart(now_);
}

void Cluster::set_partition(const std::vector<std::set<NodeId>>& groups) {
    int next = static_cast<int>(groups.size());
    std::fill(group_.begin(), group_.end(), -1);
    for (std::size_t g = 0; g < groups.size(); ++g)
        for (NodeId id : groups[g]) group_[id] = static_cast<int>(g);
    for (int& g : group_)
        if (g < 0) g = next++;  // nodes not listed are isolated on their own
}

void Cluster::heal() { std::fill(group_.begin(), group_.end(), 0); }

NodeId Cluster::leader() const {
    NodeId best = -1;
    for (int i = 0; i < size(); ++i) {
        if (!alive_[i] || nodes_[i].role() != Role::Leader) continue;
        if (best < 0 || nodes_[i].term() > nodes_[best].term()) best = i;
    }
    return best;
}

int Cluster::alive_count() const {
    return static_cast<int>(std::count(alive_.begin(), alive_.end(), true));
}

Ticket Cluster::submit(const Command& cmd) {
    Ticket t;
    const NodeId l = leader();
    if (l < 0) return t;
    Index index = 0;
    Term term = 0;
    if (!nodes_[l].submit(cmd, index, term)) return t;
    t.leader = l;
    t.index = index;
    t.term = term;
    return t;
}

Ticket Cluster::submit_to(NodeId id, const Command& cmd) {
    Ticket t;
    Index index = 0;
    Term term = 0;
    if (!alive_[id] || !nodes_[id].submit(cmd, index, term)) return t;
    t.leader = id;
    t.index = index;
    t.term = term;
    return t;
}

bool Cluster::committed(const Ticket& t) const {
    if (!t.valid()) return false;
    return applied_ref_.size() > t.index && applied_ref_[t.index].term == t.term;
}

std::optional<Result> Cluster::execute(const Command& base, int max_ticks) {
    Command cmd = base;
    cmd.client_id = 1;
    cmd.seq = ++client_seq_;  // retries reuse this seq, so the store applies it once
    const Time deadline = now_ + static_cast<Time>(max_ticks);
    while (now_ < deadline) {
        Ticket t = submit(cmd);
        if (!t.valid()) {
            step();
            continue;
        }
        const Time give_up = now_ + 120;
        while (now_ < give_up && now_ < deadline && !committed(t)) step();
        if (committed(t)) {
            for (int i = 0; i < size(); ++i) {
                if (!alive_[i] || nodes_[i].last_applied() < t.index) continue;
                auto it = nodes_[i].results().find(t.index);
                if (it != nodes_[i].results().end()) return it->second;
            }
            step();  // committed but not applied anywhere we can read yet
            for (int i = 0; i < size(); ++i) {
                if (!alive_[i] || nodes_[i].last_applied() < t.index) continue;
                auto it = nodes_[i].results().find(t.index);
                if (it != nodes_[i].results().end()) return it->second;
            }
            return Result{};
        }
    }
    return std::nullopt;
}

// ---------------------------------------------------------------- safety checks
void Cluster::check_invariants() {
    auto fail = [&](const std::string& what) {
        std::ostringstream os;
        os << "tick " << now_ << ": " << what;
        violations_.push_back(os.str());
    };

    // Election safety, and leader completeness for newly elected leaders.
    for (int i = 0; i < size(); ++i) {
        Node& n = nodes_[i];
        if (!n.became_leader_this_tick()) continue;
        n.clear_events();
        auto it = leader_of_term_.find(n.term());
        if (it != leader_of_term_.end() && it->second != i)
            fail("two leaders elected in term " + std::to_string(n.term()));
        leader_of_term_[n.term()] = i;
        for (Index k = 1; k < applied_ref_.size(); ++k) {
            if (n.last_log_index() < k || !(n.entry(k) == applied_ref_[k])) {
                fail("leader " + std::to_string(i) + " is missing committed entry " + std::to_string(k));
                break;
            }
        }
    }

    // State machine safety: every node applies the same entry at each index.
    for (int i = 0; i < size(); ++i) {
        const Node& n = nodes_[i];
        for (Index k = verified_[i] + 1; k <= n.last_applied(); ++k) {
            if (k >= applied_ref_.size()) applied_ref_.push_back(n.entry(k));
            else if (!(n.entry(k) == applied_ref_[k]))
                fail("node " + std::to_string(i) + " applied a different entry at index " + std::to_string(k));
        }
        verified_[i] = std::max(verified_[i], n.last_applied());
    }

    // Log matching, sampled every 10 ticks.
    if (now_ % 10 == 0) {
        for (int a = 0; a < size(); ++a) {
            for (int b = a + 1; b < size(); ++b) {
                Index top = std::min(nodes_[a].last_log_index(), nodes_[b].last_log_index());
                for (Index k = top; k >= 1; --k) {
                    if (nodes_[a].entry(k).term != nodes_[b].entry(k).term) continue;
                    for (Index j = 1; j <= k; ++j) {
                        if (!(nodes_[a].entry(j) == nodes_[b].entry(j))) {
                            fail("log matching violated between nodes " + std::to_string(a) + " and " +
                                 std::to_string(b) + " at index " + std::to_string(j));
                            break;
                        }
                    }
                    break;
                }
            }
        }
    }
}

}  // namespace raftkv
