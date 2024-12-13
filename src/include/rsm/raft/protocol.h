#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"

namespace chfs
{

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    // term, candidate's term
    int term;
    // candidateId requesting vote
    int candidate_id;
    // index of candidate's last log entry
    int last_log_index;
    // term of candidate's last log entry
    int last_log_term;

    // 使用 MSGPACK_DEFINE_ARRAY 宏来定义序列化和反序列化方法
    MSGPACK_DEFINE_ARRAY(term, candidate_id, last_log_index, last_log_term)
};

struct RequestVoteReply {

    // currentTerm, for candidate to update itself
    int term;
    // true means candidate received vote
    bool vote_granted;

    MSGPACK_DEFINE_ARRAY(term, vote_granted)
};

template <typename Command> struct AppendEntriesArgs {
    // leader's term
    int term;
    // so follower can redirect clients
    int leader_id;
    // index of log entry immediately preceding new ones
    int prev_log_index;
    // term of prevLogIndex entry
    int prev_log_term;
    // log entries to store (empty for heartbeat; may send more than one for efficiency)
    // In pratical, we'd better use LogEntry<Command> instead of Command
    // TODO: Implement LogEntry<Command>
    std::vector<Command> entries;
    // leader's commitIndex
    int leader_commit;

    MSGPACK_DEFINE_ARRAY(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
};

// TODO: UNCHECKED: This struct may be incorrect
struct RpcAppendEntriesArgs {

    // leader's term
    int term;
    // so follower can redirect clients
    int leader_id;
    // index of log entry immediately preceding new ones
    int prev_log_index;
    // term of prevLogIndex entry
    int prev_log_term;
    // log entries to store (empty for heartbeat; may send more than one for efficiency)
    std::vector<u8> entries;
    // leader's commitIndex
    int leader_commit;

    MSGPACK_DEFINE_ARRAY(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
};

template <typename Command> RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    return RpcAppendEntriesArgs();
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    return AppendEntriesArgs<Command>();
}

struct AppendEntriesReply {

    // currentTerm, for leader to update itself
    int term;
    // true if follower contained entry matching prevLogIndex and prevLogTerm
    bool success;

    MSGPACK_DEFINE(term, success)
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(

    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(

    )
};

} /* namespace chfs */