// ============================================================================
// TaskSystem – lock‑free, cache‑friendly (POSIX‑only) – v3.2 (MODIFIED + SHARED EXECUTION)
// ============================================================================
#pragma once

#include "Task.h"
#include "Executor.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <dlfcn.h>
#include <functional>
#include <memory>
#include <new>
#include <thread>
#include <utility>
#include <vector>
#include <array>
#include <unordered_map>
#include <condition_variable>
#include <shared_mutex>

namespace TaskSystem {

using ExecutorConstructor = Executor*(*)(std::unique_ptr<Task>);

struct TaskSystemExecutor {
    enum class TaskState {
        Waiting,
        Executing,
        Scheduled,
        Finished,
        Unknown
    };
    struct TaskID {
        uintptr_t raw{0};
        bool operator==(const TaskID& o) const noexcept { return raw == o.raw; }
        bool operator<(const TaskID& o) const noexcept { return raw < o.raw; }
    };

private:
    struct alignas(64) TaskControl {
        std::unique_ptr<Executor> exec;
        std::atomic<TaskState> state{TaskState::Waiting};
        int priority{0};
        std::atomic<uint32_t> budget{0};
        std::vector<std::function<void(TaskID)>> callbacks;
        std::atomic<bool> has_waiter{false};
        std::condition_variable_any waiter_cv;
        TaskID tid;
        std::atomic<bool> callback_fired{false};
    };

    static constexpr int MAX_PRIO = 31;
    static constexpr uint32_t QUANTUM = 512;

    struct PrioGroup {
        std::vector<TaskControl*> activeTasks;
        size_t rrIndex = 0;
        std::shared_mutex mtx;
    };

    std::array<PrioGroup, MAX_PRIO + 1> prioGroups;
    std::atomic<uint32_t> activeMask{0};

    using FnPtr = std::function<void()>*;
    std::vector<std::thread> workers;
    std::thread dispatcher;
    unsigned threadCount{0};
    std::atomic<bool> shutdown{false};
    std::unordered_map<std::string, ExecutorConstructor> executors;

    static TaskSystemExecutor* self;

    void push_task(TaskControl* tc) {
        auto& group = prioGroups[tc->priority];
        {
            std::unique_lock lock(group.mtx);
            group.activeTasks.push_back(tc);
        }
        activeMask.fetch_or(1u << tc->priority, std::memory_order_relaxed);
    }

    TaskControl* steal_from_group(PrioGroup& group) {
        std::shared_lock lock(group.mtx);
        if (group.activeTasks.empty()) return nullptr;
        size_t i = group.rrIndex++ % group.activeTasks.size();
        return group.activeTasks[i];
    }

    TaskControl* pop_highest_priority_task() {
        uint32_t mask = activeMask.load(std::memory_order_relaxed);
        if (!mask) return nullptr;
        for (int pr = MAX_PRIO; pr >= 0; --pr) {
            if (!(mask & (1u << pr))) continue;
            if (auto* tc = steal_from_group(prioGroups[pr])) return tc;
        }
        return nullptr;
    }

    void finalize(TaskControl* tc) {
        if (tc->callback_fired.exchange(true)) return; // Only once
        tc->state.store(TaskState::Finished);

        // Remove from prio group
        auto& group = prioGroups[tc->priority];
        {
            std::unique_lock lock(group.mtx);
            auto& tasks = group.activeTasks;
            tasks.erase(std::remove(tasks.begin(), tasks.end(), tc), tasks.end());
            if (tasks.empty()) {
                activeMask.fetch_and(~(1u << tc->priority), std::memory_order_relaxed);
            }
        }

        if (tc->has_waiter.load())
            tc->waiter_cv.notify_all();
        for (auto& cb : tc->callbacks) {
            auto* f = new std::function<void()>([cb, id = tc->tid] { cb(id); });
            cbq.push(f);
        }
    }

    void worker_loop(int tid) {
        while (!shutdown.load()) {
            TaskControl* tc = pop_highest_priority_task();
            if (!tc) {
                std::this_thread::yield();
                continue;
            }
            tc->state.store(TaskState::Executing);
            bool fatalError = false;
            try {
                auto status = tc->exec->ExecuteStep(tid, threadCount);
                if (status == Executor::ES_Stop) {
                    finalize(tc);
                } else {
                    tc->budget.fetch_sub(1);
                }
            } catch (const std::exception& e) {
                std::fprintf(stderr, "[T%d] Exception in ExecuteStep: %s\n", tid, e.what());
                fatalError = true;
            } catch (...) {
                std::fprintf(stderr, "[T%d] Unknown exception in ExecuteStep\n", tid);
                fatalError = true;
            }

            if (fatalError) {
                // Принудително маркираме задачата като завършена, за да не блокира системата
                finalize(tc);
            }   
        }
    }

    void dispatcher_loop() {
        while (!shutdown.load()) {
            auto* job = cbq.pop();
            if (!job) {
                std::this_thread::yield();
                continue;
            }
            (*job)();
            delete job;
        }
    }

public:
    TaskSystemExecutor(const TaskSystemExecutor&) = delete;
    TaskSystemExecutor& operator=(const TaskSystemExecutor&) = delete;

    static TaskSystemExecutor& GetInstance() { return *self; }
    static void Init(int n = 0) {
        if (n <= 0) n = std::thread::hardware_concurrency();
        delete self;
        self = new TaskSystemExecutor(n);
    }

    unsigned GetThreadCount() const { return threadCount; }

    TaskSystemExecutor(unsigned n) {
        threadCount = n;
        for (unsigned i = 0; i < threadCount; ++i) {
            workers.emplace_back([this, i] {
                worker_loop(i);
            });
        }
        dispatcher = std::thread([this] { dispatcher_loop(); });
    }

    ~TaskSystemExecutor() {
        shutdown.store(true);
        for (auto& t : workers) t.join();
        dispatcher.join();
    }

    TaskID ScheduleTask(std::unique_ptr<Task> t, int pr) {
        auto it = executors.find(t->GetExecutorName());
        assert(it != executors.end());
        ExecutorConstructor ctor = it->second;

        auto* tc = new TaskControl();
        tc->priority = std::clamp(pr, 0, MAX_PRIO);
        tc->exec.reset(ctor(std::move(t)));
        tc->budget.store(QUANTUM);
        tc->tid.raw = reinterpret_cast<uintptr_t>(tc); // взимаме адреса на TaskControl като уникален идентификатор

        push_task(tc);
        return tc->tid;
    }

    void WaitForTask(TaskID id) {
        auto* tc = reinterpret_cast<TaskControl*>(id.raw);
        if (!tc) return;
        std::mutex m;
        std::unique_lock<std::mutex> lk(m);
        tc->has_waiter.store(true);
        tc->waiter_cv.wait(lk, [&] { return tc->state.load() == TaskState::Finished; });
    }

    void OnTaskCompleted(TaskID id, std::function<void(TaskID)> cb) {
        auto* tc = reinterpret_cast<TaskControl*>(id.raw);
        if (!tc) return;
        if (tc->state.load() == TaskState::Finished) {
            if (tc->callback_fired.exchange(true)) return; // Already fired
            auto* f = new std::function<void()>([cb, id] { cb(id); });
            cbq.push(f);
            return;
        }
        tc->callbacks.emplace_back(std::move(cb));
    }

    TaskState GetTaskState(TaskID id) const {
        auto* tc = reinterpret_cast<TaskControl*>(id.raw);
        return tc ? tc->state.load() : TaskState::Unknown;
    }

    void Register(const std::string& name, ExecutorConstructor ctor) {
        executors[name] = ctor;
    }

private:
    struct CBQueue {
        static constexpr size_t CAPACITY = 1024;
        std::atomic<size_t> head{0};
        std::atomic<size_t> tail{0};
        std::array<FnPtr, CAPACITY> buffer{};

        void push(FnPtr f) {
            size_t pos;
            while (true) {
                pos = head.load(std::memory_order_relaxed);
                if ((pos - tail.load(std::memory_order_acquire)) < CAPACITY) break;
                std::this_thread::yield();
            }
            buffer[pos % CAPACITY] = f;
            head.store(pos + 1, std::memory_order_release);
        }

        FnPtr pop() {
            size_t pos;
            while (true) {
                pos = tail.load(std::memory_order_relaxed);
                if (pos >= head.load(std::memory_order_acquire)) return nullptr;
                if (tail.compare_exchange_strong(pos, pos + 1, std::memory_order_acq_rel)) {
                    return buffer[pos % CAPACITY];
                }
            }
        }
    } cbq;
};

TaskSystemExecutor* TaskSystemExecutor::self = nullptr;

} // namespace TaskSystem
