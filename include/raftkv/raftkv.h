// RaftKV: a Raft-replicated key-value store with a deterministic cluster simulator.
//
// Time is a logical tick counter. The simulated network can delay, drop and
// partition messages, and nodes can be crashed and restarted, so failure
// scenarios are exactly repeatable from a seed.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace raftkv {

using NodeId = int;
using Term = std::uint64_t;
using Index = std::uint64_t;
using Time = std::uint64_t;

enum class Role { Follower, Candidate, Leader };

struct Command {
    enum class Type { Noop, Put, Delete, Get };
    Type type = Type::Noop;
    std::string key;
    std::string value;
    std::uint64_t client_id = 0;  // 0 means "no deduplication"
    std::uint64_t seq = 0;

    bool operator==(const Command& o) const {
        return type == o.type && key == o.key && value == o.value &&
               client_id == o.client_id && seq == o.seq;
    }
};

struct LogEntry {
    Term term = 0;
    Command cmd;
    bool operator==(const LogEntry& o) const { return term == o.term && cmd == o.cmd; }
};

enum class MsgType { RequestVote, RequestVoteReply, AppendEntries, AppendEntriesReply };

struct Message {
    MsgType type = MsgType::RequestVote;
    NodeId from = -1;
    NodeId to = -1;
    Term term = 0;
    // RequestVote
    Index last_log_index = 0;
    Term last_log_term = 0;
    // RequestVoteReply
    bool vote_granted = false;
    // AppendEntries
    Index prev_log_index = 0;
    Term prev_log_term = 0;
    std::vector<LogEntry> entries;
    Index leader_commit = 0;
    // AppendEntriesReply
    bool success = false;
    Index match_index = 0;
    Index hint_index = 0;  // follower's best guess for where the logs agree
};

struct Config {
    int election_timeout_min = 15;  // ticks
    int election_timeout_max = 30;
    int heartbeat_interval = 5;
    std::size_t max_entries_per_message = 32;
};

// Messages a node wants sent; the cluster owns delivery.
using Outbox = std::vector<Message>;

// Result of applying a command to the state machine.
struct Result {
    bool ok = true;
    std::optional<std::string> value;  // for Get
};

class KvStore {
public:
    Result apply(const Command& cmd);
    std::optional<std::string> get(const std::string& key) const;
    std::size_t size() const { return data_.size(); }
    void clear();

private:
    std::map<std::string, std::string> data_;
    std::unordered_map<std::uint64_t, std::pair<std::uint64_t, Result>> last_by_client_;
};

class Node {
public:
    Node(NodeId id, int cluster_size, Config cfg, std::uint32_t seed);

    void tick(Time now, Outbox& out);
    void receive(const Message& m, Time now, Outbox& out);

    // Leader only. Appends the command and returns its position.
    bool submit(const Command& cmd, Index& index, Term& term);

    // Lose volatile state (role, commit index, state machine); keep term, vote and log.
    void crash();
    void restart(Time now);

    NodeId id() const { return id_; }
    Role role() const { return role_; }
    Term term() const { return term_; }
    Index commit_index() const { return commit_index_; }
    Index last_applied() const { return last_applied_; }
    Index last_log_index() const { return log_.size() - 1; }
    Term last_log_term() const { return log_.back().term; }
    const LogEntry& entry(Index i) const { return log_[i]; }
    const KvStore& kv() const { return kv_; }
    const std::unordered_map<Index, Result>& results() const { return results_; }
    NodeId known_leader() const { return leader_id_; }
    bool became_leader_this_tick() const { return became_leader_; }
    void clear_events() { became_leader_ = false; }

private:
    void become_follower(Term term);
    void become_leader(Time now, Outbox& out);
    void start_election(Time now, Outbox& out);
    void reset_election_deadline(Time now);
    void broadcast_append(Outbox& out);
    void send_append(NodeId peer, Outbox& out);
    void on_request_vote(const Message& m, Time now, Outbox& out);
    void on_request_vote_reply(const Message& m, Time now, Outbox& out);
    void on_append_entries(const Message& m, Time now, Outbox& out);
    void on_append_entries_reply(const Message& m, Outbox& out);
    void advance_commit();
    void apply_committed();

    NodeId id_;
    int n_;
    Config cfg_;
    std::mt19937 rng_;

    // Persistent state (survives a crash).
    Term term_ = 0;
    NodeId voted_for_ = -1;
    std::vector<LogEntry> log_;  // log_[0] is a sentinel; real entries start at 1

    // Volatile state.
    Role role_ = Role::Follower;
    NodeId leader_id_ = -1;
    Index commit_index_ = 0;
    Index last_applied_ = 0;
    Time election_deadline_ = 0;
    Time last_heartbeat_ = 0;
    int votes_ = 0;
    bool became_leader_ = false;
    std::vector<Index> next_index_;
    std::vector<Index> match_index_;
    KvStore kv_;
    std::unordered_map<Index, Result> results_;
};

// A handle for a submitted command.
struct Ticket {
    NodeId leader = -1;
    Index index = 0;
    Term term = 0;
    bool valid() const { return leader >= 0; }
};

struct NetworkConfig {
    double drop_rate = 0.0;  // probability each message is dropped
    int min_delay = 1;       // ticks
    int max_delay = 3;
};

class Cluster {
public:
    Cluster(int size, std::uint32_t seed, Config cfg = {}, NetworkConfig net = {});

    // Advance simulated time by one tick and check the safety invariants.
    void step();
    void run(int ticks);
    // Run until `pred` returns true or `max_ticks` pass. Returns whether pred held.
    template <class Pred>
    bool run_until(Pred pred, int max_ticks) {
        for (int i = 0; i < max_ticks; ++i) {
            if (pred()) return true;
            step();
        }
        return pred();
    }

    // --- fault injection
    void crash(NodeId id);
    void restart(NodeId id);
    void set_partition(const std::vector<std::set<NodeId>>& groups);
    void heal();
    void set_drop_rate(double rate) { net_.drop_rate = rate; }

    // --- queries
    int size() const { return static_cast<int>(nodes_.size()); }
    bool alive(NodeId id) const { return alive_[id]; }
    const Node& node(NodeId id) const { return nodes_[id]; }
    // The live leader with the highest term, or -1.
    NodeId leader() const;
    Time now() const { return now_; }
    int alive_count() const;

    // --- client API (talks to whichever node currently leads)
    Ticket submit(const Command& cmd);
    // Submit to a specific node (used to simulate a stale leader); invalid ticket if it is not leading.
    Ticket submit_to(NodeId id, const Command& cmd);
    bool committed(const Ticket& t) const;
    // Submit and wait for commit; retries against new leaders. Returns the result when committed.
    std::optional<Result> execute(const Command& cmd, int max_ticks = 600);

    // --- safety
    const std::vector<std::string>& violations() const { return violations_; }
    std::size_t committed_entries() const { return applied_ref_.size() > 0 ? applied_ref_.size() - 1 : 0; }

private:
    void send(Message m);
    void deliver_due();
    bool reachable(NodeId a, NodeId b) const;
    void check_invariants();

    struct Pending {
        Time at;
        std::uint64_t seq;
        Message msg;
        bool operator>(const Pending& o) const { return at != o.at ? at > o.at : seq > o.seq; }
    };

    std::vector<Node> nodes_;
    std::vector<bool> alive_;
    std::vector<int> group_;  // partition group per node; equal groups can talk
    Time now_ = 0;
    std::uint64_t msg_seq_ = 0;
    std::uint64_t client_seq_ = 0;
    std::mt19937 rng_;
    NetworkConfig net_;
    std::priority_queue<Pending, std::vector<Pending>, std::greater<Pending>> inflight_;

    // Safety bookkeeping.
    std::map<Term, NodeId> leader_of_term_;
    std::vector<LogEntry> applied_ref_{LogEntry{}};  // committed history seen so far; [0] sentinel
    std::vector<std::string> violations_;
    std::vector<Index> verified_;  // per node: highest applied index already checked
};

}  // namespace raftkv
