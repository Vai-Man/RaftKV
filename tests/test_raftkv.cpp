// Dependency-free tests: protocol unit tests on a single Node, and the
// fault-injection scenarios across many seeds.
#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "raftkv/raftkv.h"
#include "raftkv/scenarios.h"

using namespace raftkv;

namespace {

int g_failures = 0;
int g_checks = 0;
std::vector<std::pair<std::string, std::function<void()>>>& registry() {
    static std::vector<std::pair<std::string, std::function<void()>>> r;
    return r;
}
struct Register {
    Register(const char* name, std::function<void()> fn) { registry().emplace_back(name, std::move(fn)); }
};

#define CHECK(cond)                                                                       \
    do {                                                                                  \
        ++g_checks;                                                                       \
        if (!(cond)) {                                                                    \
            ++g_failures;                                                                 \
            std::cout << "  CHECK failed: " #cond " (" << __FILE__ << ":" << __LINE__ << ")\n"; \
        }                                                                                 \
    } while (0)
#define TEST(name)                                  \
    static void name();                             \
    static Register reg_##name(#name, name);        \
    static void name()

Command put(const std::string& k, const std::string& v, std::uint64_t client = 0, std::uint64_t seq = 0) {
    Command c;
    c.type = Command::Type::Put;
    c.key = k;
    c.value = v;
    c.client_id = client;
    c.seq = seq;
    return c;
}

LogEntry entry(Term t, const std::string& key) {
    LogEntry e;
    e.term = t;
    e.cmd = put(key, "v");
    return e;
}

Message append(NodeId from, Term term, Index prev, Term prev_term, std::vector<LogEntry> entries, Index commit = 0) {
    Message m;
    m.type = MsgType::AppendEntries;
    m.from = from;
    m.to = 0;
    m.term = term;
    m.prev_log_index = prev;
    m.prev_log_term = prev_term;
    m.entries = std::move(entries);
    m.leader_commit = commit;
    return m;
}

Message vote(NodeId from, Term term, Index last_index, Term last_term) {
    Message m;
    m.type = MsgType::RequestVote;
    m.from = from;
    m.to = 0;
    m.term = term;
    m.last_log_index = last_index;
    m.last_log_term = last_term;
    return m;
}

Node fresh_node() { return Node(0, 3, Config{}, 1); }

}  // namespace

// ------------------------------------------------------------- state machine
TEST(kv_put_get_delete) {
    KvStore kv;
    Command c = put("a", "1");
    kv.apply(c);
    CHECK(kv.get("a") == std::optional<std::string>("1"));
    Command d;
    d.type = Command::Type::Delete;
    d.key = "a";
    CHECK(kv.apply(d).ok);
    CHECK(!kv.get("a").has_value());
}

TEST(kv_delete_missing_key_reports_not_ok) {
    KvStore kv;
    Command d;
    d.type = Command::Type::Delete;
    d.key = "nope";
    CHECK(!kv.apply(d).ok);
}

TEST(kv_get_returns_value_through_apply) {
    KvStore kv;
    kv.apply(put("k", "v"));
    Command g;
    g.type = Command::Type::Get;
    g.key = "k";
    auto r = kv.apply(g);
    CHECK(r.ok && r.value && *r.value == "v");
}

TEST(kv_repeated_client_seq_is_applied_once) {
    KvStore kv;
    kv.apply(put("k", "first", 7, 1));
    kv.apply(put("k", "second", 7, 1));  // retry of the same request with different payload is ignored
    CHECK(kv.get("k") == std::optional<std::string>("first"));
}

TEST(kv_new_client_seq_is_applied) {
    KvStore kv;
    kv.apply(put("k", "first", 7, 1));
    kv.apply(put("k", "second", 7, 2));
    CHECK(kv.get("k") == std::optional<std::string>("second"));
}

TEST(kv_clients_are_independent) {
    KvStore kv;
    kv.apply(put("a", "1", 1, 1));
    kv.apply(put("b", "2", 2, 1));
    CHECK(kv.size() == 2);
}

// ------------------------------------------------------------- single-node protocol rules
TEST(node_grants_only_one_vote_per_term) {
    Node n = fresh_node();
    Outbox out;
    n.receive(vote(1, 1, 0, 0), 0, out);
    CHECK(out.back().vote_granted);
    n.receive(vote(2, 1, 0, 0), 0, out);
    CHECK(!out.back().vote_granted);
}

TEST(node_repeats_vote_for_same_candidate) {
    Node n = fresh_node();
    Outbox out;
    n.receive(vote(1, 1, 0, 0), 0, out);
    n.receive(vote(1, 1, 0, 0), 0, out);
    CHECK(out.back().vote_granted);
}

TEST(node_rejects_vote_from_stale_term) {
    Node n = fresh_node();
    Outbox out;
    n.receive(vote(1, 5, 0, 0), 0, out);
    n.receive(vote(2, 3, 0, 0), 0, out);
    CHECK(!out.back().vote_granted);
    CHECK(out.back().term == 5);
}

TEST(node_denies_vote_to_candidate_with_older_log) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 2, 0, 0, {entry(2, "x")}), 0, out);
    n.receive(vote(2, 3, 5, 1), 0, out);  // longer log but older last term
    CHECK(!out.back().vote_granted);
}

TEST(node_grants_vote_to_candidate_with_newer_log) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 1, 0, 0, {entry(1, "x")}), 0, out);
    n.receive(vote(2, 2, 1, 2), 0, out);  // same length, newer last term
    CHECK(out.back().vote_granted);
}

TEST(node_rejects_append_from_stale_term) {
    Node n = fresh_node();
    Outbox out;
    n.receive(vote(1, 5, 0, 0), 0, out);
    n.receive(append(2, 3, 0, 0, {entry(3, "x")}), 0, out);
    CHECK(!out.back().success);
    CHECK(out.back().term == 5);
    CHECK(n.last_log_index() == 0);
}

TEST(node_rejects_append_with_missing_prev_entry) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 1, 5, 1, {entry(1, "x")}), 0, out);
    CHECK(!out.back().success);
    CHECK(out.back().hint_index == 0);
    CHECK(n.last_log_index() == 0);
}

TEST(node_rejects_append_with_mismatched_prev_term) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 1, 0, 0, {entry(1, "a"), entry(1, "b")}), 0, out);
    n.receive(append(2, 2, 2, 2, {entry(2, "c")}), 0, out);  // prev term 2, we have 1
    CHECK(!out.back().success);
    CHECK(n.last_log_index() == 2);
}

TEST(node_truncates_conflicting_entries) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 1, 0, 0, {entry(1, "a"), entry(1, "b"), entry(1, "c")}), 0, out);
    CHECK(n.last_log_index() == 3);
    n.receive(append(2, 2, 1, 1, {entry(2, "z")}), 0, out);
    CHECK(out.back().success);
    CHECK(n.last_log_index() == 2);
    CHECK(n.last_log_term() == 2);
}

TEST(node_keeps_matching_suffix_on_duplicate_append) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 1, 0, 0, {entry(1, "a"), entry(1, "b"), entry(1, "c")}), 0, out);
    n.receive(append(1, 1, 0, 0, {entry(1, "a")}), 0, out);  // delayed duplicate of an earlier message
    CHECK(out.back().success);
    CHECK(n.last_log_index() == 3);
}

TEST(node_commit_follows_leader_commit_and_applies) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 1, 0, 0, {entry(1, "a"), entry(1, "b"), entry(1, "c")}, 2), 0, out);
    CHECK(n.commit_index() == 2);
    CHECK(n.last_applied() == 2);
    CHECK(n.kv().get("b").has_value());
    CHECK(!n.kv().get("c").has_value());
}

TEST(node_never_commits_past_its_own_log) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 1, 0, 0, {entry(1, "a")}, 9), 0, out);
    CHECK(n.commit_index() == 1);
}

TEST(node_crash_keeps_term_vote_and_log_but_not_state_machine) {
    Node n = fresh_node();
    Outbox out;
    n.receive(append(1, 4, 0, 0, {entry(4, "a"), entry(4, "b")}, 2), 0, out);
    n.crash();
    CHECK(n.term() == 4);
    CHECK(n.last_log_index() == 2);
    CHECK(n.commit_index() == 0);
    CHECK(n.kv().size() == 0);
}

TEST(node_only_leader_accepts_submissions) {
    Node n = fresh_node();
    Index i = 0;
    Term t = 0;
    CHECK(!n.submit(put("k", "v"), i, t));
}

TEST(node_follower_becomes_candidate_after_timeout) {
    Node n = fresh_node();
    Outbox out;
    n.tick(1000, out);
    CHECK(n.role() == Role::Candidate);
    CHECK(n.term() == 1);
    CHECK(out.size() == 2);  // vote requests to the two peers
}

// ------------------------------------------------------------- small clusters
TEST(single_node_cluster_commits_alone) {
    Cluster c(1, 3);
    CHECK(c.run_until([&] { return c.leader() == 0; }, 200));
    CHECK(c.execute(put("a", "1")).has_value());
    CHECK(c.node(0).kv().get("a") == std::optional<std::string>("1"));
}

TEST(three_node_cluster_tolerates_one_failure) {
    Cluster c(3, 5);
    CHECK(c.run_until([&] { return c.leader() >= 0; }, 300));
    c.crash(c.leader());
    CHECK(c.run_until([&] { return c.leader() >= 0; }, 600));
    CHECK(c.execute(put("a", "1")).has_value());
    CHECK(c.violations().empty());
}

TEST(minority_partition_never_elects_a_leader) {
    Cluster c(5, 9);
    CHECK(c.run_until([&] { return c.leader() >= 0; }, 300));
    c.set_partition({{0, 1, 2}, {3, 4}});
    c.run(600);
    CHECK(c.node(3).role() != Role::Leader);
    CHECK(c.node(4).role() != Role::Leader);
    CHECK(c.violations().empty());
}

// ------------------------------------------------------------- scenarios across seeds
static void run_scenario_for_seeds(ScenarioResult (*fn)(unsigned), int seeds) {
    for (int s = 1; s <= seeds; ++s) {
        ScenarioResult r = fn(static_cast<unsigned>(s));
        ++g_checks;
        if (!r.passed) {
            ++g_failures;
            std::cout << "  scenario " << r.name << " failed for seed " << s << ": " << r.detail << "\n";
        }
    }
}

TEST(scenario_initial_election_many_seeds) { run_scenario_for_seeds(scenario_initial_election, 25); }
TEST(scenario_leader_crash_many_seeds) { run_scenario_for_seeds(scenario_leader_crash, 25); }
TEST(scenario_follower_catch_up_many_seeds) { run_scenario_for_seeds(scenario_follower_catch_up, 25); }
TEST(scenario_partitioned_leader_many_seeds) { run_scenario_for_seeds(scenario_partitioned_leader, 25); }
TEST(scenario_no_quorum_many_seeds) { run_scenario_for_seeds(scenario_no_quorum, 25); }
TEST(scenario_lossy_network_many_seeds) { run_scenario_for_seeds(scenario_lossy_network, 25); }
TEST(scenario_rolling_leader_crashes_many_seeds) { run_scenario_for_seeds(scenario_rolling_leader_crashes, 25); }
TEST(scenario_divergent_logs_many_seeds) { run_scenario_for_seeds(scenario_divergent_logs, 25); }

int main() {
    int failed_tests = 0;
    for (auto& [name, fn] : registry()) {
        const int before = g_failures;
        fn();
        const bool ok = g_failures == before;
        std::cout << (ok ? "[ ok ] " : "[FAIL] ") << name << "\n";
        if (!ok) ++failed_tests;
    }
    std::cout << "\n" << registry().size() << " tests, " << g_checks << " checks, " << failed_tests << " failed\n";
    return failed_tests ? 1 : 0;
}
