// Fault-injection scenarios, shared by the demo binary and the tests.
#pragma once

#include <string>
#include <vector>

namespace raftkv {

struct ScenarioResult {
    std::string name;
    std::string description;
    bool passed = false;
    std::string detail;            // why it failed, or a short summary
    unsigned long long ticks = 0;  // simulated time used
    unsigned long long committed = 0;  // log entries committed (including leader no-ops)
};

ScenarioResult scenario_initial_election(unsigned seed);
ScenarioResult scenario_leader_crash(unsigned seed);
ScenarioResult scenario_follower_catch_up(unsigned seed);
ScenarioResult scenario_partitioned_leader(unsigned seed);
ScenarioResult scenario_no_quorum(unsigned seed);
ScenarioResult scenario_lossy_network(unsigned seed);
ScenarioResult scenario_rolling_leader_crashes(unsigned seed);
ScenarioResult scenario_divergent_logs(unsigned seed);

std::vector<ScenarioResult> run_all_scenarios(unsigned seed);

}  // namespace raftkv
