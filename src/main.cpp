// Runs the fault-injection scenarios and prints a summary table.
#include <cstdlib>
#include <iomanip>
#include <iostream>

#include "raftkv/scenarios.h"

int main(int argc, char** argv) {
    unsigned seed = argc > 1 ? static_cast<unsigned>(std::strtoul(argv[1], nullptr, 10)) : 1;
    std::cout << "RaftKV fault-injection scenarios (5 nodes, seed " << seed << ")\n\n";
    int failed = 0;
    for (const auto& r : raftkv::run_all_scenarios(seed)) {
        std::cout << std::left << std::setw(24) << r.name << (r.passed ? "PASS" : "FAIL") << "  "
                  << std::right << std::setw(6) << r.ticks << " ticks  " << std::setw(4) << r.committed
                  << " entries  " << r.description;
        if (!r.passed) std::cout << "\n    -> " << r.detail;
        std::cout << "\n";
        if (!r.passed) ++failed;
    }
    std::cout << "\n" << (failed ? "FAILED: " : "all scenarios passed") << (failed ? std::to_string(failed) : "") << "\n";
    return failed ? 1 : 0;
}
