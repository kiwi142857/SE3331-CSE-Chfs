#include "distributed/client.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rpc/msgpack.hpp"
#include <mutex>
#include <string>
#include <utility>
#include <vector>

// Lab4: Free to modify this file

namespace mapReduce
{
struct KeyVal {
    KeyVal(const std::string &key, const std::string &val) : key(key), val(val)
    {
    }
    KeyVal()
    {
    }
    std::string key;
    std::string val;
};

enum mr_tasktype {
    NONE = 0,
    MAP,
    REDUCE
};

struct Task {
    int index;
    mr_tasktype type;
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    bool is_assigned;
    bool is_completed;
    char start_char;
    char end_char;
};

std::vector<KeyVal> Map(const std::string &content);

std::string Reduce(const std::string &key, const std::vector<std::string> &values);

const std::string ASK_TASK = "ask_task";
const std::string SUBMIT_TASK = "submit_task";

struct MR_CoordinatorConfig {
    uint16_t port;
    std::string ip_address;
    std::string resultFile;
    std::shared_ptr<chfs::ChfsClient> client;

    MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                         std::string resultFile)
        : port(port), ip_address(std::move(ip_address)), resultFile(resultFile), client(std::move(client))
    {
    }
};

class SequentialMapReduce
{
  public:
    SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files,
                        std::string resultFile);
    void doWork();

  private:
    std::shared_ptr<chfs::ChfsClient> chfs_client;
    std::vector<std::string> files;
    std::string outPutFile;
};

struct RpcTaskInfo {
    int type;
    int index;
    std::string filename;
    char start_char;
    char end_char;
    unsigned long file_num;

    MSGPACK_DEFINE_ARRAY(type, index, filename, start_char, end_char, file_num);
};

class Coordinator
{
  public:
    Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
    RpcTaskInfo askTask(int);
    int submitTask(int taskType, int index);
    bool Done();

  private:
    std::vector<std::string> files;
    std::mutex mtx;
    bool isFinished;
    int nReduce;
    std::vector<Task> map_tasks;
    std::vector<Task> reduce_tasks;
    Task merge_task;
    std::unique_ptr<chfs::RpcServer> rpc_server;
    void checkTimeoutTasks();
};

class Worker
{
  public:
    explicit Worker(MR_CoordinatorConfig config);
    void doWork();
    void stop();

  private:
    void doMap(int index, const std::string &filename);
    void doReduce(int index, int nfiles, char start_char, char end_char);
    void doMerge();
    void doSubmit(mr_tasktype taskType, int index);

    std::string outPutFile;
    std::unique_ptr<chfs::RpcClient> mr_client;
    std::shared_ptr<chfs::ChfsClient> chfs_client;
    std::unique_ptr<std::thread> work_thread;
    bool shouldStop = false;
    int nReduce;
};
} // namespace mapReduce