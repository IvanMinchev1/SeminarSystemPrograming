#include "TaskSystem.hpp"
#include "Executor.h"
#include "Task.h"
#include <cstdio>
#include <thread>
#include <chrono>
#include <memory>
#include <iostream>
#include <random>
#include <map>
#include <mutex>
#include <unordered_map>
#include <iomanip>

using namespace TaskSystem;
using Clock = std::chrono::steady_clock;

std::mutex stats_mutex;
std::map<std::string, std::vector<long long>> exec_durations;
std::unordered_map<int, std::unordered_map<std::string, int>> thread_work;

void record_duration(const std::string& label, long long ms) {
    std::lock_guard<std::mutex> lk(stats_mutex);
    exec_durations[label].push_back(ms);
}

void record_thread(int tid, const std::string& label) {
    std::lock_guard<std::mutex> lk(stats_mutex);
    thread_work[tid][label]++;
}

const char* color_for(int value) {
    if (value == 0) return "\033[0m";
    else if (value < 5) return "\033[38;5;245m";
    else if (value < 10) return "\033[38;5;214m";
    else if (value < 20) return "\033[38;5;178m";
    else if (value < 40) return "\033[38;5;82m";
    else if (value < 80) return "\033[38;5;27m";
    else return "\033[38;5;196m";
}

void print_statistics() {
    std::puts("\n==== Execution Stats ====");
    for (const auto& [label, times] : exec_durations) {
        long long sum = 0, max = 0, min = LLONG_MAX;
        for (auto t : times) {
            sum += t;
            if (t > max) max = t;
            if (t < min) min = t;
        }
        long long avg = times.empty() ? 0 : sum / times.size();
        std::printf("[%s] count=%zu avg=%lld ms min=%lld ms max=%lld ms\n",
            label.c_str(), times.size(), avg, min, max);
    }

    std::puts("\n==== Thread Workload ====");
    for (const auto& [tid, counts] : thread_work) {
        std::printf("[T%02d]", tid);
        for (const auto& [label, cnt] : counts)
            std::printf(" %s:%d", label.c_str(), cnt);
        std::puts("");
    }

    std::puts("\n==== Thread Heatmap (colored cells) ====");
    std::map<std::string, std::vector<int>> per_task;
    int threadCount = TaskSystemExecutor::GetInstance().GetThreadCount();
    for (const auto& [tid, counts] : thread_work) {
        for (const auto& [label, cnt] : counts) {
            auto& vec = per_task[label];
            if ((int)vec.size() <= tid)
                vec.resize(tid + 1);
            vec[tid] += cnt;
        }
    }
    std::printf("[  Task   ]");
    for (int tid = 0; tid < threadCount; ++tid)
        std::printf(" T%02d", tid);
    std::puts("");

    for (const auto& [label, vec] : per_task) {
        std::printf("[%10s]", label.c_str());
        for (int tid = 0; tid < threadCount; ++tid) {
            int v = (tid < (int)vec.size()) ? vec[tid] : 0;
            const char* col = color_for(v);
            std::printf(" %s%4d\033[0m", col, v);
        }
        std::puts("");
    }
    std::puts("==========================\n");
}

struct PrinterTask : public Task {
    int start = 0, end = 0;
    std::string label;
    std::string GetExecutorName() const override { return "Printer"; }
};

struct PrinterExec : public Executor {
    std::atomic<int> cur;
    int end;
    std::string label;
    PrinterExec(std::unique_ptr<Task> t) : Executor(std::move(t)) {
        auto* pt = static_cast<PrinterTask*>(task.get());
        cur = pt->start;
        end = pt->end;
        label = pt->label;
    }
    ExecStatus ExecuteStep(int tid, int) override {
        int val = cur++;
        if (val > end) return ES_Stop;
        std::printf("[T%d Printer:%s] %d\n", tid, label.c_str(), val);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        record_thread(tid, "Printer");
        return ES_Continue;
    }
};
static Executor* MakePrinter(std::unique_ptr<Task> t) { return new PrinterExec(std::move(t)); }

struct RTTask : public Task {
    int rays = 100000;
    std::string GetExecutorName() const override { return "Raytracer"; }
};

struct RTExec : public Executor {
    std::atomic<int> left;
    RTExec(std::unique_ptr<Task> t) : Executor(std::move(t)) {
        left = static_cast<RTTask*>(task.get())->rays;
    }
    ExecStatus ExecuteStep(int tid, int) override {
        if (left <= 0) return ES_Stop;
        int remain = left.fetch_sub(1000);
        if (remain <= 0) return ES_Stop;
        std::printf("[T%d Raytracer] step (%d left)\n", tid, remain);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        record_thread(tid, "Raytracer");
        return ES_Continue;
    }
};
static Executor* MakeRT(std::unique_ptr<Task> t) { return new RTExec(std::move(t)); }

struct CrunchTask : public Task {
    int size = 1000000;
    std::string GetExecutorName() const override { return "Cruncher"; }
};

struct CrunchExec : public Executor {
    std::atomic<int> remaining;
    CrunchExec(std::unique_ptr<Task> t) : Executor(std::move(t)) {
        remaining = static_cast<CrunchTask*>(task.get())->size;
    }
    ExecStatus ExecuteStep(int tid, int) override {
        int rem = remaining.fetch_sub(50000);
        if (rem <= 0) return ES_Stop;
        std::printf("[T%d Crunch] %d left\n", tid, rem);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        record_thread(tid, "Crunch");
        return ES_Continue;
    }
};
static Executor* MakeCrunch(std::unique_ptr<Task> t) { return new CrunchExec(std::move(t)); }

int main() {
    TaskSystemExecutor::Init();
    auto& ts = TaskSystemExecutor::GetInstance();

    ts.Register("Printer", &MakePrinter);
    ts.Register("Raytracer", &MakeRT);
    ts.Register("Cruncher", &MakeCrunch);

    std::vector<TaskSystemExecutor::TaskID> ids;
    std::map<TaskSystemExecutor::TaskID, Clock::time_point> start_times;

    auto schedule = [&](std::unique_ptr<Task> task, const std::string& label, int prio, const std::string& kind, int index) {
        auto id = ts.ScheduleTask(std::move(task), prio);
        start_times[id] = Clock::now();
        ts.OnTaskCompleted(id, [=, &start_times](TaskSystemExecutor::TaskID) {
            auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_times.at(id)).count();
            std::printf("[Callback] %s %d done (%lld ms)\n", kind.c_str(), index, dur);
            record_duration(kind, dur);
        });
        ids.push_back(id);
    };

    for (int i = 0; i < 3; ++i) {
        auto p = std::make_unique<PrinterTask>();
        p->start = 1;
        p->end = 10 + i * 5;
        p->label = "P" + std::to_string(i);
        schedule(std::move(p), p->label, (i % 2 == 0) ? 5 : 7, "Printer", i);
    }

    for (int i = 0; i < 4; ++i) {
        auto r = std::make_unique<RTTask>();
        r->rays = 30000 + i * 15000;
        schedule(std::move(r), "R" + std::to_string(i), 8 + (i % 2), "Raytracer", i);
    }

    for (int i = 0; i < 4; ++i) {
        auto c = std::make_unique<CrunchTask>();
        c->size = 700000 + i * 400000;
        schedule(std::move(c), "C" + std::to_string(i), 6 + (i % 3), "Crunch", i);
    }

    for (auto id : ids) ts.WaitForTask(id);

    std::puts("[Main] All tasks complete.");
    print_statistics();
    return 0;
}
