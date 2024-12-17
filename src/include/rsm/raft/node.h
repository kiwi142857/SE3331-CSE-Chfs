#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <stdarg.h>
#include <thread>
#include <unistd.h>

#include "block/manager.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/config.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"

namespace chfs
{

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command> class RaftNode
{

// 定义宏来控制是否重定向 std::cerr
#define REDIRECT_CERR_TO_FILE
// 定义宏来控制是否打开日志
#define RAFT_WARNING_ON
// 定义宏来控制是否打开警告
#define RAFT_LOG_ON
// 定义宏来控制是否打开错误
#define RAFT_ERROR_ON
// 定义宏来控制是否打开调试
#define RAFT_DEBUG_ON

#ifdef REDIRECT_CERR_TO_FILE
#define SET_CERR_OUTPUT(file)                                                                                          \
    do {                                                                                                               \
        static std::ofstream ofs(file);                                                                                \
        if (ofs.is_open()) {                                                                                           \
            std::cerr.rdbuf(ofs.rdbuf());                                                                              \
        } else {                                                                                                       \
            std::cerr << "Failed to open log file: " << file << std::endl;                                             \
        }                                                                                                              \
    } while (0)
#else
#define SET_CERR_OUTPUT(file)                                                                                          \
    do {                                                                                                               \
    } while (0)
#endif

#ifdef RAFT_LOG_ON
#define RAFT_LOG(fmt, args...)                                                                                         \
    do {                                                                                                               \
        auto now =                                                                                                     \
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
                .count();                                                                                              \
        char buf[512];                                                                                                 \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term,  \
                role, ##args);                                                                                         \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                             \
    } while (0);
#else
#define RAFT_LOG(fmt, args...)
#endif

#ifdef RAFT_WARNING_ON
// use iostream to print debug info
#define RAFT_WARNING(fmt, args...)                                                                                     \
    do {                                                                                                               \
        auto now =                                                                                                     \
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
                .count();                                                                                              \
        char buf[512];                                                                                                 \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] WARNING " fmt "\n", now, __FILE__, __LINE__, my_id,        \
                current_term, role, ##args);                                                                           \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                             \
    } while (0);
#else
#define RAFT_WARNING(fmt, args...)
#endif

#ifdef RAFT_ERROR_ON
// use iostream to print debug info
#define RAFT_ERROR(fmt, args...)                                                                                       \
    do {                                                                                                               \
        auto now =                                                                                                     \
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
                .count();                                                                                              \
        char buf[512];                                                                                                 \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] !!!ERROR!!! " fmt "\n", now, __FILE__, __LINE__, my_id,    \
                current_term, role, ##args);                                                                           \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                             \
    } while (0);
#else
#define RAFT_ERROR(fmt, args...)
#endif

#ifdef RAFT_DEBUG_ON
// use iostream to print debug info
#define RAFT_DEBUG(fmt, args...)                                                                                       \
    do {                                                                                                               \
        auto now =                                                                                                     \
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
                .count();                                                                                              \
        char buf[512];                                                                                                 \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] DEBUG " fmt "\n", now, __FILE__, __LINE__, my_id,          \
                current_term, role, ##args);                                                                           \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                             \
    } while (0);
#else
#define RAFT_DEBUG(fmt, args...)
#endif

  public:
    RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

  private:
    /*
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;

    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /*
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;

    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* send heart_beat: Only be called when new election is done */
    void send_heartbeats();

    /* update commit index */
    void update_commit_index();

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    /* Data structures */
    bool network_stat; /* for test */

    std::mutex mtx;         /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
    int my_id; /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    int voted_for;  // ID of the candidate that received vote in current term (or -1 if none)
    int vote_count; // Number of votes received in current election

    std::chrono::time_point<std::chrono::system_clock> last_heartbeat; // Last time we received a heartbeat
    std::chrono::milliseconds election_timeout;                        // Election timeout duration
    std::vector<int> nextIndex;  // For each server, index of the next log entry to send to that server
    std::vector<int> matchIndex; // For each server, index of highest log entry known to be replicated on server
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs)
    : network_stat(true), node_configs(configs), my_id(node_id), stopped(true), role(RaftRole::Follower),
      current_term(0), leader_id(-1), vote_count(0), last_heartbeat(std::chrono::system_clock::now())
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                     [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT,
                     [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

    /* Lab3: Your code here */

    // Initialise log storage
    log_storage = std::make_unique<RaftLog<Command>>(std::make_shared<BlockManager>(block_file));

    // Initialise state machine
    state = std::make_unique<StateMachine>();

    vote_count = 0;
    election_timeout = std::chrono::milliseconds(rand() % election_timeout_range + election_timeout_base);
    nextIndex.resize(node_configs.size(), 1);
    matchIndex.resize(node_configs.size(), 0);
    rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command> RaftNode<StateMachine, Command>::~RaftNode()
{
    RAFT_LOG("Destructor called");

    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command> auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    if (!stopped) {
        return 0; // Already started
    }
    stopped = false;

    // Initialise thread pool
    thread_pool = std::make_unique<ThreadPool>(thread_pool_size);

    SET_CERR_OUTPUT("log.txt");
    RAFT_LOG("Starting node");

    // RaftLog for persistence
    std::string node_log_filename = raft_log_folder + node_file_subpath + std::to_string(my_id);
    bool is_recovery = is_file_exist(node_log_filename);
    auto block_manager = std::shared_ptr<BlockManager>(new BlockManager(node_log_filename));
    log_storage = std::make_unique<RaftLog<Command>>(block_manager, is_recovery);

    // log_storage->save_metadata();
    // log_storage->set_snapshot_term(-1);
    // log_storage->recover();

    // Initialize RPC clients for all nodes
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (const auto &config : node_configs) {
            if (config.node_id != my_id) {
                rpc_clients_map[config.node_id] = std::make_unique<RpcClient>(config.ip_address, config.port, true);
            }
        }
    }

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command> auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    RAFT_LOG("Stopping node");
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (stopped) {
            return 0; // Already stopped
        }
        stopped = true;
        RAFT_LOG("Set stopped to true");
    }

    // 通知所有后台线程停止
    if (background_election && background_election->joinable()) {
        background_election->join();
    }
    if (background_ping && background_ping->joinable()) {
        background_ping->join();
    }
    if (background_commit && background_commit->joinable()) {
        background_commit->join();
    }
    if (background_apply && background_apply->joinable()) {
        background_apply->join();
    }

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    if (role == RaftRole::Leader) {
        return std::make_tuple(true, current_term);
    } else {
        return std::make_tuple(false, current_term);
    }
}

template <typename StateMachine, typename Command> auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    std::unique_lock<std::mutex> lock(mtx);
    RAFT_LOG("Received new command");
    // If not the leader, return false
    if (role != RaftRole::Leader) {
        return std::make_tuple(false, current_term, -1);
    }

    // Create a new command
    // Print the cmd_data here
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);
    RaftLogEntry<Command> entry(current_term, cmd);
    // Print cmd
    RAFT_LOG("NEW COMMAND: Received new command, value = %d", cmd.value);

    // Append the command to the log
    int log_index = log_storage->append_entry(current_term, cmd);
    RAFT_LOG("Appended new command to log at index %d", log_index);

    // Send AppendEntries RPCs to all followers
    // for (const auto &config : node_configs) {
    //     if (config.node_id != my_id) {
    //         AppendEntriesArgs<Command> args;
    //         args.term = current_term;
    //         args.leader_id = my_id;
    //         // args.prev_log_index = log_storage->last_log_index() - 1;
    //         // TODO: check here
    //         args.prev_log_index = log_storage->last_log_index() - 1;
    //         args.prev_log_term = log_storage->term(args.prev_log_index);
    //         args.entries = {entry};
    //         args.leader_commit = log_storage->commit_index();

    //         RAFT_LOG("NEW COMMAND: Sending AppendEntries RPC to node %d, term %d, prev_log_index %d, prev_log_term
    //         %d, "
    //                  "entries %d, leader_commit %d, value[0]= %d",
    //                  config.node_id, args.term, args.prev_log_index, args.prev_log_term,
    //                  static_cast<int>(args.entries.size()), args.leader_commit,
    //                  args.entries.empty() ? -1 : args.entries.front().command().value);

    //         thread_pool->enqueue(&RaftNode::send_append_entries, this, config.node_id, args);
    //     }
    // }

    return std::make_tuple(true, current_term, log_index);
}

template <typename StateMachine, typename Command> auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return std::vector<u8>();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    // TODO: In TA's document, it says that I may add commited log index in RequestVoteArgs.
    // But in the code, it only has last_log_index and last_log_term.
    RAFT_LOG("Received RequestVote RPC from node %d", args.candidate_id);

    std::unique_lock<std::mutex> lock(mtx);

    RequestVoteReply reply;
    reply.term = current_term;
    reply.vote_granted = false;

    // Step 1: Reply false if term < currentTerm (§5.1)
    if (args.term < current_term) {
        return reply;
    }

    // Step 2: If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (§5.1)
    if (args.term > current_term) {
        current_term = args.term;
        if (role != RaftRole::Follower) {
            RAFT_LOG("args.term > current_term, convert to follower; original role: %d", role);
            role = RaftRole::Follower;
            // Reset voted_for and other state variables
        }
        voted_for = -1;
        log_storage->save_metadata(current_term, voted_for);
    }

    // Step 3: If votedFor is null or candidateId, and candidate’s log is at
    // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
    bool log_is_up_to_date =
        (args.last_log_term > log_storage->last_log_term()) ||
        (args.last_log_term == log_storage->last_log_term() && args.last_log_index >= log_storage->last_log_index());

    if ((voted_for == -1 || voted_for == args.candidate_id) && log_is_up_to_date) {
        voted_for = args.candidate_id;
        reply.vote_granted = true;
        log_storage->save_metadata(current_term, voted_for);
    }

    RAFT_LOG("Voting for node %d, term %d, vote_granted %d", args.candidate_id, reply.term, reply.vote_granted);
    return reply;
}

template <typename StateMachine, typename Command> void RaftNode<StateMachine, Command>::send_heartbeats()
{
    RAFT_LOG("Sending heartbeats to all nodes");
    for (const auto &config : node_configs) {
        if (config.node_id != my_id) {
            RAFT_LOG("Sending heartbeat to node %d", config.node_id);
            AppendEntriesArgs<Command> args;
            args.term = current_term;
            args.leader_id = my_id;
            // args.prev_log_index = log_storage->last_log_index();
            // TODO: check here
            args.prev_log_index = nextIndex[config.node_id] - 1;
            args.prev_log_term = log_storage->term(args.prev_log_index);
            args.entries = {}; // Empty entries for heartbeat
            args.leader_commit = log_storage->commit_index();

            RAFT_LOG("Sending heartbeat to node %d, term %d, prev_log_index %d, prev_log_term %d, entries %d, "
                     "leader_commit %d",
                     config.node_id, args.term, args.prev_log_index, args.prev_log_term,
                     static_cast<int>(args.entries.size()), args.leader_commit);
            thread_pool->enqueue(&RaftNode::send_append_entries, this, config.node_id, args);
        }
    }
}

template <typename StateMachine, typename Command> void RaftNode<StateMachine, Command>::update_commit_index()
{
    for (int N = log_storage->last_log_index(); N > log_storage->commit_index(); --N) {
        int count = 1; // Count this node
        for (const auto &config : node_configs) {
            if (config.node_id != my_id && matchIndex[config.node_id] >= N) {
                count++;
            }
        }
        // The current_term match is required to ensure that the commit index is only updated for the current term
        if (count > node_configs.size() / 2 && log_storage->term(N) == current_term) {
            RAFT_LOG("Leader updating commit index to %d", N);
            log_storage->set_commit_index(N);
            break;
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                                                const RequestVoteReply reply)
{
    RAFT_LOG("Received RequestVoteReply RPC from node %d, term %d, vote_granted %d", target, reply.term,
             reply.vote_granted);
    // Warning: I notice that the function will be only called when lock is being held. So we don't need to lock again.
    std::unique_lock<std::mutex> lock(mtx);

    // Step 1: If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (§5.1)
    if (reply.term > current_term) {
        current_term = reply.term;
        RAFT_LOG("reply.term > current_term, convert to follower; original role: %d", role);

        role = RaftRole::Follower;
        voted_for = -1;
        log_storage->save_metadata(current_term, voted_for);
        return;
    }

    // Step 2: If we are not a candidate, ignore the reply
    if (role != RaftRole::Candidate) {
        RAFT_LOG("Not a candidate, ignore the reply");
        return;
    }

    // Step 3: Count the votes
    if (reply.vote_granted) {
        RAFT_LOG("Node %d has voted for us", target);
        vote_count++;
    }

    // Step 4: If votes received from majority of servers: become leader
    if (vote_count > node_configs.size() / 2) {
        RAFT_LOG("Received majority of votes, convert to leader; original role: %d", role);
        role = RaftRole::Leader;

        // TODO: Initialize leader state (e.g., nextIndex, matchIndex)
        for (int i = 0; i < node_configs.size(); i++) {
            nextIndex[i] = log_storage->last_log_index() + 1;
            matchIndex[i] = 0;
        }
        // Start sending heartbeats to followers
        thread_pool->enqueue(&RaftNode::send_heartbeats, this);
    }

    RAFT_LOG("Finished handling RequestVoteReply RPC from node %d, term %d, vote_granted %d", target, reply.term,
             reply.vote_granted);
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    RAFT_LOG("Received AppendEntries RPC from node %d, term %d, prev_log_index %d, prev_log_term %d, entries num %d, "
             "leader_commit %d",
             rpc_arg.leader_id, rpc_arg.term, rpc_arg.prev_log_index, rpc_arg.prev_log_term,
             static_cast<int>(rpc_arg.entries.size() / sizeof(RaftLogEntry<Command>)), rpc_arg.leader_commit);

    std::unique_lock<std::mutex> lock(mtx);

    AppendEntriesReply reply;
    reply.term = current_term;
    reply.success = false;

    // update last_heartbeat
    last_heartbeat = std::chrono::system_clock::now();

    // Step 1: Reply false if term < currentTerm (§5.1)
    if (rpc_arg.term < current_term) {
        return reply;
    }

    // Step 2: If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (§5.1)
    if (rpc_arg.term > current_term) {
        current_term = rpc_arg.term;
        RAFT_LOG("rpc_arg.term > current_term, convert to follower; original role: %d", role);
        role = RaftRole::Follower;
        voted_for = -1;
        log_storage->save_metadata(current_term, voted_for);
    }

    // Step 3: Reply false if log doesn’t contain an entry at prevLogIndex
    // whose term matches prevLogTerm (§5.3)
    if (!log_storage->contain_index(rpc_arg.prev_log_index)) {
        RAFT_LOG("Log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm");
        return reply;
    }

    // Step 4: If an existing entry conflicts with a new one (same index
    // but different terms), delete the existing entry and all that follow it (§5.3)
    if (!log_storage->match_log(rpc_arg.prev_log_index, rpc_arg.prev_log_term)) {
        RAFT_LOG("An existing entry conflicts with a new one, delete the existing entry and all that follow it, "
                 "rpc_arg.prev_log_index = %d, rpc_arg.prev_log_term = %d, my term = %d",
                 rpc_arg.prev_log_index, rpc_arg.prev_log_term, log_storage->term(rpc_arg.prev_log_index));
        log_storage->truncate_log(rpc_arg.prev_log_index);
        // Check again to see if this truncate is enough
        if (!log_storage->match_log(rpc_arg.prev_log_index, rpc_arg.prev_log_term)) {
            RAFT_LOG("Truncate not enough, return false");
            return reply;
        }
    }

    // Step 5: Append any new entries not already in the log
    // FOR DEBUG TMP
    if (rpc_arg.entries.size() > sizeof(RaftLogEntry<Command>)) {
        RAFT_DEBUG("rpc_arg.entries.size() = %zu", rpc_arg.entries.size() / sizeof(RaftLogEntry<Command>));
        // values are as below( print in one line )
        std::stringstream ss;
        Command cmd;
        size_t entry_size = cmd.size();
        for (size_t i = 0; i < rpc_arg.entries.size(); i += entry_size) {
            std::vector<u8> entry_data(rpc_arg.entries.begin() + i, rpc_arg.entries.begin() + i + entry_size);
            cmd.deserialize(entry_data, entry_size);
            ss << cmd.value << ' ';
        }
        RAFT_DEBUG("rpc_arg.entries: %s", ss.str().c_str());
    }

    // TODO: if more than one entry per time, we may change code below
    if ((!rpc_arg.entries.empty()) &&
        (rpc_arg.prev_log_index + rpc_arg.entries.size()) > log_storage->last_log_index()) {
        Command cmd;
        RaftLogEntry<Command> entry;
        size_t entry_size = entry.size();
        size_t cmd_size = cmd.size();
        for (size_t i = 0; i < rpc_arg.entries.size(); i += entry_size) {
            RaftLogEntry<Command> entry(rpc_arg.entries, i, cmd_size);
            log_storage->append_entry(entry);
            RAFT_LOG("Appended new entry to log at index %d, term %d, value %d", log_storage->last_log_index(),
                     log_storage->last_log_term(), entry.command().value);
        }
    }

    // Step 6: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (§5.3)
    if (rpc_arg.leader_commit > log_storage->commit_index()) {
        log_storage->set_commit_index(std::min(rpc_arg.leader_commit, log_storage->last_log_index()));
        RAFT_LOG("Set commitIndex to %d", log_storage->commit_index());
    }
    reply.success = true;
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg,
                                                                  const AppendEntriesReply reply)
{
    RAFT_LOG("Received AppendEntriesReply RPC from node %d, term %d, success %d", node_id, reply.term, reply.success);
    // Warning: I notice that the function will be only called when lock is being held. So we don't need to lock again.
    std::unique_lock<std::mutex> lock(mtx);

    // Step 1: If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (§5.1)
    if (reply.term > current_term) {
        current_term = reply.term;
        role = RaftRole::Follower;
        RAFT_LOG("reply.term > current_term, convert to follower; original role: %d", role);
        voted_for = -1;
        log_storage->save_metadata(current_term, voted_for);
        return;
    }

    // Step 2: If we are not the leader, ignore the reply
    if (role != RaftRole::Leader) {
        return;
    }

    // Step 3: If AppendEntries RPC was successful, update nextIndex and matchIndex for the follower
    if (reply.success) {
        // FOR DEBUG
        // if (node_id == 2) {
        //     RAFT_DEBUG("nextIndex[2] = %d, nextIndex[2] will be set to %d", nextIndex[2],
        //                std::max(arg.prev_log_index + static_cast<int>(arg.entries.size()) + 1, nextIndex[node_id]));
        // }
        // int origin_nextIndex = nextIndex[node_id];

        nextIndex[node_id] =
            std::max(arg.prev_log_index + static_cast<int>(arg.entries.size()) + 1, nextIndex[node_id]);
        matchIndex[node_id] = nextIndex[node_id] - 1;
    } else {
        // FOR DEBUG TMP
        int origin_nextIndex = nextIndex[node_id];
        // If AppendEntries RPC failed, decrement nextIndex and retry
        // FOR DEBUG
        // if (node_id == 2) {
        //     RAFT_DEBUG("nextIndex[2] = %d, nextIndex[2] will be set to %d", nextIndex[2],
        //                std::max(1, nextIndex[node_id] - 1));
        // }

        nextIndex[node_id] = std::max(1, nextIndex[node_id] - 1);
        AppendEntriesArgs<Command> retry_args;
        retry_args.prev_log_index = nextIndex[node_id] - 1;
        retry_args.prev_log_term = log_storage->term(retry_args.prev_log_index);
        retry_args.term = current_term;
        retry_args.entries = log_storage->get_entries(retry_args.prev_log_index + 1,
                                                      log_storage->last_log_index() - retry_args.prev_log_index);
        retry_args.leader_commit = log_storage->commit_index();
        // FOR DEBUG TEMP
        // print entries value here, to print in one line for easy debug, we using a stream to contain all the value
        // with ' ' as split
        std::stringstream ss;
        for (const auto &entry : retry_args.entries) {
            ss << entry.command().value << ' ';
        }
        RAFT_DEBUG("Retry with value: %s, original nextIndex = %d, new nextIndex = %d", ss.str().c_str(),
                   origin_nextIndex, nextIndex[node_id]);
        thread_pool->enqueue(&RaftNode::send_append_entries, this, node_id, retry_args);
    }

    // Step 4: If there exists an N such that N > commitIndex, a majority
    // of matchIndex[i] ≥ N, and log[N].term == currentTerm, set commitIndex = N (§5.3, §5.4)
    // TODO: check if this commit logic is correct
    update_commit_index();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    return InstallSnapshotReply();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg,
                                                                    const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    RAFT_LOG("Send request vote to %d", target_id);
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr ||
        rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        if (rpc_clients_map[target_id] == nullptr) {
            RAFT_WARNING("rpc_clients_map[%d] is nullptr", target_id);
        } else {
            RAFT_WARNING("rpc_clients_map[%d] is not connected", target_id);
        }
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
        RAFT_ERROR("RPC fails");
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr ||
        rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        if (rpc_clients_map[target_id] == nullptr) {
            RAFT_WARNING("rpc_clients_map[%d] is nullptr", target_id);
        } else {
            RAFT_WARNING("rpc_clients_map[%d] is not connected", target_id);
        }
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
        RAFT_LOG("ERROR: RPC fails");
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr ||
        rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        if (rpc_clients_map[target_id] == nullptr) {
            RAFT_WARNING("rpc_clients_map[%d] is nullptr", target_id);
        } else {
            RAFT_WARNING("rpc_clients_map[%d] is not connected", target_id);
        }
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command> void RaftNode<StateMachine, Command>::run_background_election()
{
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (is_stopped()) {
                return;
            }

            // Only work for followers and candidates
            if (role == RaftRole::Leader) {
                continue;
            }

            // Check if we need to start a new election
            auto now = std::chrono::system_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat);
            // TODO: should I update last_heartbeat here?

            if (elapsed >= election_timeout) {
                // Start a new election
                RAFT_LOG("Timeout, convert to candidate; original role: %d", role);
                role = RaftRole::Candidate;
                current_term++;
                voted_for = my_id;
                vote_count = 1; // Vote for self
                RAFT_LOG("Start election");

                // Send RequestVote RPCs to all other nodes
                RequestVoteArgs args;
                args.term = current_term;
                args.candidate_id = my_id;
                args.last_log_index = log_storage->last_log_index();
                args.last_log_term = log_storage->last_log_term();
                log_storage->save_metadata(current_term, voted_for);

                for (const auto &config : node_configs) {
                    if (config.node_id != my_id) {
                        RAFT_LOG("Enqueue request vote task for node %d", config.node_id);
                        thread_pool->enqueue(&RaftNode::send_request_vote, this, config.node_id, args);
                    }
                }

                // Actually, I think it doesn't matter if we donn't reset election_timeout here.
                // Reset election timeout
                last_heartbeat = std::chrono::system_clock::now();
                election_timeout = std::chrono::milliseconds(rand() % election_timeout_range + election_timeout_base);
            }
        }

        // Sleep for a short duration before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(run_background_election_sleep));
    }
}

template <typename StateMachine, typename Command> void RaftNode<StateMachine, Command>::run_background_commit()
{
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (is_stopped()) {
                return;
            }
            // Only work for the leader
            if (role != RaftRole::Leader) {
                continue;
            }

            // Check if there are any new entries to commit
            update_commit_index();

            // send logs to the follower
            for (const auto &config : node_configs) {
                if (config.node_id == my_id) {
                    continue;
                }
                if (nextIndex[config.node_id] <= log_storage->last_log_index()) {
                    AppendEntriesArgs<Command> args;
                    args.term = current_term;
                    args.leader_id = my_id;
                    args.prev_log_index = nextIndex[config.node_id] - 1;
                    args.prev_log_term = log_storage->term(args.prev_log_index);
                    args.entries = log_storage->get_entries(nextIndex[config.node_id],
                                                            log_storage->last_log_index() - args.prev_log_index);
                    args.leader_commit = log_storage->commit_index();
                    RAFT_LOG("Sending AppendEntries RPC to node %d, term %d, prev_log_index %d, prev_log_term %d, "
                             "entries %d, leader_commit %d",
                             config.node_id, args.term, args.prev_log_index, args.prev_log_term,
                             static_cast<int>(args.entries.size()), args.leader_commit);
                    thread_pool->enqueue(&RaftNode::send_append_entries, this, config.node_id, args);
                }
            }
        }
        // Sleep for a short duration before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(run_background_commit_sleep));
    }

    return;
}

template <typename StateMachine, typename Command> void RaftNode<StateMachine, Command>::run_background_apply()
{
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    int last_applied = 0; // Initialize last_applied to 0

    while (true) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (is_stopped()) {
                return;
            }

            // Apply all committed but not yet applied entries to the state machine
            while (log_storage->commit_index() > last_applied) {
                last_applied++;
                Command cmd = log_storage->get_entry(last_applied).command();
                RAFT_LOG("Applying log at index %d to state machine with value %d", last_applied, cmd.value);
                state->apply_log(cmd);
            }
        }

        // Sleep for a short duration before checking again
        std::this_thread::sleep_for(std::chrono::milliseconds(run_background_apply_sleep));
    }

    return;
}

template <typename StateMachine, typename Command> void RaftNode<StateMachine, Command>::run_background_ping()
{
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mtx);
            if (is_stopped()) {
                return;
            }

            // Only work for the leader
            if (role != RaftRole::Leader) {
                continue;
            }

            send_heartbeats();
        }

        // Sleep for a short duration before sending the next heartbeat
        std::this_thread::sleep_for(std::chrono::milliseconds(run_background_ping_sleep));
    }
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client : rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network : network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config : node_configs) {
                if (config.node_id == node_id)
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command> void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client : rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command> int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command> int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client : rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }

    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot();
}

} // namespace chfs