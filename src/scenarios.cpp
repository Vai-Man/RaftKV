#include "raftkv/scenarios.h"

#include <set>
#include <sstream>

#include "raftkv/raftkv.h"

namespace raftkv {
namespace {

Command put(const std::string& k, const std::string& v) {
    Command c;
    c.type = Command::Type::Put;
    c.key = k;
    c.value = v;
    return c;
}

Command get(const std::string& k) {
    Command c;
    c.type = Command::Type::Get;
    c.key = k;
    return c;
}

// Run on a healed, fully alive cluster until every live node has applied the same prefix.
bool settle(Cluster& c, int max_ticks = 1500) {
    return c.run_until(
        [&] {
            const NodeId l = c.leader();
            if (l < 0) return false;
            const Index target = c.node(l).commit_index();
            if (target == 0 || c.node(l).last_applied() < target) return false;
            for (int i = 0; i < c.size(); ++i) {
                if (!c.alive(i)) continue;
                if (c.node(i).last_applied() != target || c.node(i).kv().size() != c.node(l).kv().size())
                    return false;
            }
            return true;
        },
        max_ticks);
}

bool read_equals(Cluster& c, const std::string& key, const std::string& want) {
    auto r = c.execute(get(key));
    return r && r->ok && r->value && *r->value == want;
}

ScenarioResult finish(ScenarioResult r, const Cluster& c, bool ok, const std::string& why) {
    const bool safe = c.violations().empty();
    r.passed = ok && safe;
    r.ticks = c.now();
    r.committed = c.committed_entries();
    if (!safe) r.detail = "safety violation: " + c.violations().front();
    else if (!ok) r.detail = why;
    else r.detail = "ok";
    return r;
}

ScenarioResult begin(const char* name, const char* description) {
    ScenarioResult r;
    r.name = name;
    r.description = description;
    return r;
}

}  // namespace

ScenarioResult scenario_initial_election(unsigned seed) {
    auto r = begin("initial-election", "5 nodes elect exactly one leader, then replicate 10 writes");
    Cluster c(5, seed);
    if (!c.run_until([&] { return c.leader() >= 0; }, 400)) return finish(r, c, false, "no leader elected");
    for (int i = 0; i < 10; ++i)
        if (!c.execute(put("k" + std::to_string(i), "v" + std::to_string(i))))
            return finish(r, c, false, "write did not commit");
    if (!settle(c)) return finish(r, c, false, "nodes did not converge");
    return finish(r, c, read_equals(c, "k7", "v7"), "read-back failed");
}

ScenarioResult scenario_leader_crash(unsigned seed) {
    auto r = begin("leader-crash", "crash the leader; a new one is elected and the old leader catches up");
    Cluster c(5, seed);
    c.run_until([&] { return c.leader() >= 0; }, 400);
    for (int i = 0; i < 5; ++i) c.execute(put("a" + std::to_string(i), "1"));
    const NodeId old = c.leader();
    c.crash(old);
    if (!c.run_until([&] { return c.leader() >= 0 && c.leader() != old; }, 600))
        return finish(r, c, false, "no new leader after crash");
    for (int i = 0; i < 5; ++i)
        if (!c.execute(put("b" + std::to_string(i), "2"))) return finish(r, c, false, "write after failover failed");
    c.restart(old);
    if (!settle(c)) return finish(r, c, false, "restarted node did not catch up");
    return finish(r, c, read_equals(c, "a3", "1") && read_equals(c, "b3", "2"), "data lost across failover");
}

ScenarioResult scenario_follower_catch_up(unsigned seed) {
    auto r = begin("follower-catch-up", "a follower is down for 20 writes, restarts and catches up");
    Cluster c(5, seed);
    c.run_until([&] { return c.leader() >= 0; }, 400);
    const NodeId down = (c.leader() + 1) % 5;
    c.crash(down);
    for (int i = 0; i < 20; ++i)
        if (!c.execute(put("k" + std::to_string(i), std::to_string(i)))) return finish(r, c, false, "write failed");
    c.restart(down);
    if (!settle(c)) return finish(r, c, false, "follower did not catch up");
    const bool same = c.node(down).kv().get("k19") == std::optional<std::string>("19");
    return finish(r, c, same, "follower state differs");
}

ScenarioResult scenario_partitioned_leader(unsigned seed) {
    auto r = begin("partitioned-leader", "isolated leader cannot commit; the majority elects another and wins on heal");
    Cluster c(5, seed);
    c.run_until([&] { return c.leader() >= 0; }, 400);
    c.execute(put("x", "before"));
    const NodeId old = c.leader();
    const NodeId buddy = (old + 1) % 5;
    std::set<NodeId> minority{old, buddy}, majority;
    for (int i = 0; i < 5; ++i)
        if (!minority.count(i)) majority.insert(i);
    c.set_partition({minority, majority});

    // The old leader still believes it leads and accepts a write it can never commit.
    Command stale_cmd = put("x", "stale");
    stale_cmd.client_id = 99;
    stale_cmd.seq = 1;
    const Ticket stale = c.submit_to(old, stale_cmd);

    if (!c.run_until([&] { return c.leader() >= 0 && majority.count(c.leader()) > 0; }, 800))
        return finish(r, c, false, "majority did not elect a leader");
    if (!c.execute(put("x", "after"))) return finish(r, c, false, "majority could not commit");
    c.heal();
    if (!settle(c)) return finish(r, c, false, "cluster did not converge after heal");
    const bool stale_lost = !c.committed(stale);
    return finish(r, c, stale_lost && read_equals(c, "x", "after"), "stale write survived or final value wrong");
}

ScenarioResult scenario_no_quorum(unsigned seed) {
    auto r = begin("no-quorum", "with only 2 of 5 nodes alive nothing commits; recovery resumes progress");
    Cluster c(5, seed);
    c.run_until([&] { return c.leader() >= 0; }, 400);
    c.execute(put("k", "v1"));
    c.crash(0);
    c.crash(1);
    c.crash(2);
    if (c.execute(put("k", "v2"), 300)) return finish(r, c, false, "committed without a quorum");
    c.restart(0);
    if (!c.execute(put("k", "v3"), 1500)) return finish(r, c, false, "no progress after quorum returned");
    c.restart(1);
    c.restart(2);
    if (!settle(c)) return finish(r, c, false, "did not converge");
    return finish(r, c, read_equals(c, "k", "v3"), "wrong value after recovery");
}

ScenarioResult scenario_lossy_network(unsigned seed) {
    auto r = begin("lossy-network", "20% message loss: 30 writes still commit and nodes agree");
    NetworkConfig net;
    net.drop_rate = 0.2;
    Cluster c(5, seed, Config{}, net);
    if (!c.run_until([&] { return c.leader() >= 0; }, 1500)) return finish(r, c, false, "no leader");
    for (int i = 0; i < 30; ++i)
        if (!c.execute(put("k" + std::to_string(i), std::to_string(i * i)), 3000))
            return finish(r, c, false, "write did not commit under loss");
    c.set_drop_rate(0.0);
    if (!settle(c)) return finish(r, c, false, "did not converge");
    return finish(r, c, read_equals(c, "k9", "81"), "read-back failed");
}

ScenarioResult scenario_rolling_leader_crashes(unsigned seed) {
    auto r = begin("rolling-leader-crashes", "the leader is crashed six times in a row; no acknowledged write is lost");
    Cluster c(5, seed);
    c.run_until([&] { return c.leader() >= 0; }, 400);
    NodeId previous = -1;
    for (int round = 0; round < 6; ++round) {
        if (!c.execute(put("round" + std::to_string(round), "done"), 1500))
            return finish(r, c, false, "write failed in round " + std::to_string(round));
        const NodeId l = c.leader();
        c.crash(l);
        if (previous >= 0) c.restart(previous);
        previous = l;
        if (!c.run_until([&] { return c.leader() >= 0 && c.leader() != previous; }, 800))
            return finish(r, c, false, "no new leader in round " + std::to_string(round));
    }
    c.restart(previous);
    if (!settle(c)) return finish(r, c, false, "did not converge");
    for (int round = 0; round < 6; ++round)
        if (!read_equals(c, "round" + std::to_string(round), "done")) return finish(r, c, false, "acknowledged write lost");
    return finish(r, c, true, "");
}

ScenarioResult scenario_divergent_logs(unsigned seed) {
    auto r = begin("divergent-logs", "a deposed leader's uncommitted entries are overwritten after the partition heals");
    Cluster c(5, seed);
    c.run_until([&] { return c.leader() >= 0; }, 400);
    c.execute(put("base", "1"));
    const NodeId old = c.leader();
    std::set<NodeId> alone{old}, rest;
    for (int i = 0; i < 5; ++i)
        if (i != old) rest.insert(i);
    c.set_partition({alone, rest});

    // Three entries pile up on the isolated leader's log.
    for (int i = 0; i < 3; ++i) {
        Command cmd = put("lost" + std::to_string(i), "x");
        cmd.client_id = 98;
        cmd.seq = static_cast<std::uint64_t>(i + 1);
        c.submit_to(old, cmd);
    }
    const Index diverged = c.node(old).last_log_index();

    if (!c.run_until([&] { return c.leader() >= 0 && c.leader() != old; }, 800))
        return finish(r, c, false, "majority did not elect a leader");
    for (int i = 0; i < 4; ++i)
        if (!c.execute(put("kept" + std::to_string(i), "y"))) return finish(r, c, false, "majority could not commit");
    c.heal();
    if (!settle(c)) return finish(r, c, false, "did not converge after heal");
    const bool dropped = !c.node(old).kv().get("lost0").has_value();
    const bool replaced = c.node(old).kv().get("kept3") == std::optional<std::string>("y");
    std::ostringstream why;
    why << "old leader still holds diverged entries (log had " << diverged << ")";
    return finish(r, c, dropped && replaced, why.str());
}

std::vector<ScenarioResult> run_all_scenarios(unsigned seed) {
    return {
        scenario_initial_election(seed),   scenario_leader_crash(seed),
        scenario_follower_catch_up(seed),  scenario_partitioned_leader(seed),
        scenario_no_quorum(seed),          scenario_lossy_network(seed),
        scenario_rolling_leader_crashes(seed), scenario_divergent_logs(seed),
    };
}

}  // namespace raftkv
