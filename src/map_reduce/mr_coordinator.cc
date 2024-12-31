#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "map_reduce/protocol.h"

namespace mapReduce
{
RpcTaskInfo Coordinator::askTask(int)
{
    // Lab4 : Your code goes here.
    // Free to change the type of return value.
    std::unique_lock<std::mutex> uniqueLock(this->mtx);
    checkTimeoutTasks();

    for (auto &task : map_tasks) {
        if (!task.is_assigned ||
            (task.is_assigned && !task.is_completed &&
             std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - task.start_time)
                     .count() > 10)) {
            task.is_assigned = true;
            task.start_time = std::chrono::steady_clock::now();
            return RpcTaskInfo{task.type, task.index, files[task.index], '\0', '\0', files.size()};
        }
    }

    // TODO: We need to check if all map tasks are completed
    // We may create a variable to store the number of completed map tasks or is_all_map_tasks_completed
    bool all_map_completed =
        std::all_of(map_tasks.begin(), map_tasks.end(), [](const Task &task) { return task.is_completed; });

    if (all_map_completed) {
        for (auto &task : reduce_tasks) {
            if (!task.is_assigned ||
                (task.is_assigned && !task.is_completed &&
                 std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - task.start_time)
                         .count() > 10)) {
                task.is_assigned = true;
                task.start_time = std::chrono::steady_clock::now();
                return RpcTaskInfo{task.type, task.index, "", task.start_char, task.end_char, files.size()};
            }
        }
    }

    bool all_reduce_completed =
        std::all_of(reduce_tasks.begin(), reduce_tasks.end(), [](const Task &task) { return task.is_completed; });

    if (all_reduce_completed && !merge_task.is_assigned) {
        merge_task.is_assigned = true;
        merge_task.start_time = std::chrono::steady_clock::now();
        return RpcTaskInfo{REDUCE, -1, "", '\0', '\0', files.size()};
    }

    return RpcTaskInfo{NONE, -1, "", '\0', '\0', files.size()};
}

int Coordinator::submitTask(int taskType, int index)
{
    // Lab4 : Your code goes here.
    std::unique_lock<std::mutex> uniqueLock(this->mtx);

    if (taskType == MAP) {
        map_tasks[index].is_completed = true;
    } else if (taskType == REDUCE) {
        if (index == -1 && merge_task.is_assigned) {
            merge_task.is_completed = true;
            isFinished = true;
        } else {
            reduce_tasks[index].is_completed = true;
        }
    }

    return 0;
}

// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
bool Coordinator::Done()
{
    std::unique_lock<std::mutex> uniqueLock(this->mtx);
    return this->isFinished;
}

void Coordinator::checkTimeoutTasks()
{
    for (auto &task : map_tasks) {
        if (task.is_assigned && !task.is_completed &&
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - task.start_time)
                    .count() > 10) {
            task.is_assigned = false;
        }
    }

    for (auto &task : reduce_tasks) {
        if (task.is_assigned && !task.is_completed &&
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - task.start_time)
                    .count() > 10) {
            task.is_assigned = false;
        }
    }

    if (merge_task.is_assigned && !merge_task.is_completed &&
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - merge_task.start_time)
                .count() > 10) {
        merge_task.is_assigned = false;
    }
}

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce)
    : files(files), isFinished(false), nReduce(nReduce)
{
    // Lab4: Your code goes here (Optional).
    for (int i = 0; i < files.size(); ++i) {
        map_tasks.push_back({i, MAP, std::chrono::steady_clock::now(), false, false, '\0', '\0'});
    }
    int range_size = 26 / this->nReduce;
    char start_char = 'a';
    for (int i = 0; i < nReduce; ++i) {
        char end_char = (i == nReduce - 1) ? 'z' : start_char + range_size - 1;
        reduce_tasks.push_back({i, REDUCE, std::chrono::steady_clock::now(), false, false, start_char, end_char});
        start_char = end_char + 1;
    }

    merge_task = {0, REDUCE, std::chrono::steady_clock::now(), false, false, '\0', '\0'};

    rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
    rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
    rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
    rpc_server->run(true, 1);
}
} // namespace mapReduce