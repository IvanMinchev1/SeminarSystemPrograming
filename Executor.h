#pragma once

#include "Task.h"

#include <memory>
#include "Executor.h"
#include "TaskSystem.hpp"
#include <memory>
#include <string>
#include <atomic>
#include <vector>
#include <array>
#include <unordered_map>
#include <functional>
#include <condition_variable>
#include <cassert>
#include <cstdint>
#include <thread>
#include <chrono>
#include <cstdio>
namespace TaskSystem {


/**
 * @brief Base class for task executor. Should be inherited in executor plugins
 *
 */
struct Executor {
    enum ExecStatus {
        ES_Continue, ES_Stop
    };

    Executor(std::unique_ptr<Task> taskToExecute) : task(std::move(taskToExecute)) {}
    virtual ~Executor() {}

    /**
     * @brief Execute a small step of the task, on a given thread. TaskSystem is allowed to call this method multiple times
     *        even after it has returned ES_Stop once
     *
     * @param threadIndex the current thread index, in range [0, threadCount - 1]
     * @param threadCount the total number of threads running
     * @return ExecStatus return ES_Stop when task is finished, returns ES_Continue otherwise
     */
    virtual ExecStatus ExecuteStep(int threadIndex, int threadCount) = 0;

    std::unique_ptr<Task> task;
};

/**
 * @brief Type of the needed function in the dynamic library for executor creation
 *
 */
typedef Executor*(*ExecutorConstructor)(std::unique_ptr<Task> taskToExecute);
struct TaskSystemExecutor;


};

/**
 * @brief Each executor library must define OnLibraryInit function that will be called once on load
 *        It should be used to register the executor in the TaskSystemExecutor
 *
 */
typedef void (*OnLibraryInitPtr)(TaskSystem::TaskSystemExecutor &);

#if defined(_WIN32) || defined(_WIN64)
#define DLL_EXPORT __declspec(dllexport)
#else
#define DLL_EXPORT
#endif

#define IMPLEMENT_ON_INIT() extern "C" DLL_EXPORT void OnLibraryInit(TaskSystem::TaskSystemExecutor &ts)