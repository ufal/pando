// P4.1d: the process worker pool (borrow idle workers, never wait) and the CPU budget.
//
// Run: cmake --build build --target worker_pool_test && ./build/worker_pool_test

#include "core/worker_pool.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace pando;

static int failures = 0;
#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            std::cerr << __FILE__ << ":" << __LINE__ << ": CHECK failed: " #cond "\n"; \
            ++failures; \
        } \
    } while (0)

int main() {
    CHECK(usable_cpus() >= 1);
    CHECK(usable_cpus() <= std::max(1u, std::thread::hardware_concurrency()));

    // every task exactly once, with and without helpers
    for (unsigned helpers : {0u, 1u, 3u, 16u}) {
        WorkerPool pool(4);
        std::vector<std::atomic<int>> hit(1000);
        pool.run(hit.size(), helpers, [&](size_t i) { hit[i].fetch_add(1); });
        bool once = true;
        for (auto& h : hit) once = once && h.load() == 1;
        CHECK(once);
    }

    // helpers capped by max_helpers: at most max_helpers + 1 distinct threads
    {
        WorkerPool pool(8);
        std::mutex mu;
        std::set<std::thread::id> ids;
        pool.run(64, 2, [&](size_t) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            std::lock_guard<std::mutex> lk(mu);
            ids.insert(std::this_thread::get_id());
        });
        CHECK(ids.size() >= 1 && ids.size() <= 3);
        const auto st = pool.stats();
        CHECK(st.steps == 1);
        CHECK(st.helpers_wanted == 2);
        CHECK(st.helpers_granted <= 2);
        CHECK(st.busy == 0);
    }

    // never waits: with every worker busy, a second step runs on its caller alone
    {
        WorkerPool pool(1);
        std::atomic<bool> release{false}, in_first{false};
        std::thread t([&] {
            pool.run(2, 1, [&](size_t) {
                in_first = true;
                while (!release) std::this_thread::sleep_for(std::chrono::milliseconds(1));
            });
        });
        while (!in_first) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));   // the worker joined the first step
        const auto t0 = std::chrono::steady_clock::now();
        std::atomic<int> n{0};
        pool.run(10, 1, [&](size_t) { ++n; });
        CHECK(n == 10);
        CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(1));
        release = true;
        t.join();
    }

    // nested steps (a range's lexicon scan) do not deadlock
    {
        WorkerPool pool(2);
        std::atomic<int> n{0};
        pool.run(4, 2, [&](size_t) { pool.run(8, 2, [&](size_t) { ++n; }); });
        CHECK(n == 32);
    }

    // an exception reaches the caller after the other tasks finished
    {
        WorkerPool pool(3);
        std::atomic<int> n{0};
        bool thrown = false;
        try {
            pool.run(20, 3, [&](size_t i) {
                ++n;
                if (i == 5) throw std::runtime_error("x");
            });
        } catch (const std::runtime_error&) {
            thrown = true;
        }
        CHECK(thrown);
        CHECK(n == 20);
        CHECK(pool.stats().busy == 0);
    }

    // configure: the first explicit size wins
    {
        WorkerPool pool;
        CHECK(pool.configure(3));
        CHECK(pool.size() == 3);
        CHECK(pool.configure(3));
        CHECK(!pool.configure(5));
        CHECK(pool.size() == 3);
    }

    // the request's thread budget is per thread and nests
    CHECK(current_thread_budget() == 0);
    {
        ThreadBudgetScope a(4);
        CHECK(current_thread_budget() == 4);
        {
            ThreadBudgetScope b(1);
            CHECK(current_thread_budget() == 1);
            std::thread([] { CHECK(current_thread_budget() == 0); }).join();
        }
        CHECK(current_thread_budget() == 4);
    }
    CHECK(current_thread_budget() == 0);

    if (failures) {
        std::cerr << failures << " failure(s)\n";
        return 1;
    }
    std::cout << "worker_pool_test: OK (usable CPUs " << usable_cpus() << ")\n";
    return 0;
}
