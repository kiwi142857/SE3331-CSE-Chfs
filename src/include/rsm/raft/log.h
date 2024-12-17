#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include "config.h"
#include "filesystem/operations.h"
#include "util.h"
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

namespace chfs
{

template <typename Command> class RaftLogEntry
{
  public:
    RaftLogEntry() : term_(0), command_(Command())
    {
    } // Default constructor
    RaftLogEntry(int term, const Command &command) : term_(term), command_(command)
    {
    }
    RaftLogEntry(std::vector<u8> data, int offset, int size)
    {
        // since the offset, the first 4 bytes are the term
        term_ = (data[offset] << 24) | (data[offset + 1] << 16) | (data[offset + 2] << 8) | data[offset + 3];
        // the rest of the data is the command
        command_.deserialize(std::vector<u8>(data.begin() + offset + 4, data.begin() + offset + 4 + size), size);
    }
    ~RaftLogEntry()
    {
    }

    int term() const
    {
        return term_;
    }
    Command command() const
    {
        return command_;
    }
    size_t size() const
    {
        return 4 + command_.size();
    }

  private:
    int term_;
    Command command_;
};

/**
 * RaftLog uses a BlockManager to manage the data.
 */
template <typename Command> class RaftLog
{
  public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    RaftLog(std::shared_ptr<BlockManager> bm, bool is_recovery);

    ~RaftLog();

    // Append new entries to the log
    void append_entries(const std::vector<Command> &entries);

    // Append a single entry to the log
    int append_entry(int term, const Command &entry);

    // Append a single entry to the log
    int append_entry(const RaftLogEntry<Command> &entry);

    // Check if the log contains an entry at the given index with the given term
    bool match_log(int index, int term) const;

    // Check if the log contains an entry at the given index
    bool contain_index(int index) const;

    // Truncate the log from the given index
    void truncate_log(int index);

    // Get the index of the last log entry
    int last_log_index() const;

    // Get the term of the last log entry
    int last_log_term() const;

    // Get the current commit index
    int commit_index() const;

    // Set the commit index
    void set_commit_index(int index);

    // log_storage->term(N) is the term of the Nth log entry
    // we define function term(N) to be the term of the Nth log entry
    int term(int N) const;

    // Get the entry at the given index
    RaftLogEntry<Command> get_entry(int index) const;

    // Get the entries from the given index and length
    std::vector<RaftLogEntry<Command>> get_entries(int index, int length) const;

    // Set the current term
    void set_current_term(int term);

    // Get the current term
    int current_term() const;

    // Set the voted for candidate
    void set_voted_for(int candidate_id);

    // Get the voted for candidate
    int voted_for() const;

    // Save the log to the given file
    void save_log() const;

    // Save metadata to the given file
    void save_metadata() const;

    // Save snapshot to the given file
    void save_snapshot() const;

    // Get the snapshot
    std::vector<u8> get_snapshot() const;

    // Set the snapshot
    void set_snapshot(const std::vector<u8> &snapshot);

    // Get the snapshot index
    int get_snapshot_index() const;

    // Set the snapshot index
    void set_snapshot_index(int index);

    // Get the snapshot term
    int get_snapshot_term() const;

    // Set the snapshot term
    void set_snapshot_term(int term);

    // Recover
    void recover();

  private:
    std::shared_ptr<BlockManager> bm_;
    std::shared_ptr<FileOperation> file_op_;
    mutable std::mutex mtx;
    std::vector<RaftLogEntry<Command>> log_entries; // Vector to store log entries
    int commit_idx;                                 // Index of the highest log entry known to be committed
    int current_term_;                              // Current term
    int voted_for_;                                 // Candidate ID that received vote in current term
    const int max_inode_num;                        // Maximum number of inodes
    std::vector<u8> node_snapshot;                  // Snapshot of the node
    int snapshot_index;                             // Index of the snapshot
    int snapshot_term;                              // Term of the snapshot

    // Recover the log from the given file
    void recover_log();

    // Recover metadata from the given file
    void recover_metadata();

    // Recover snapshot from the given file
    void recover_snapshot();
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm)
    : bm_(bm), commit_idx(0), current_term_(0), max_inode_num(max_inode_num_), voted_for(-1)
{
    // Append an empty log entry at the beginning
    log_entries.emplace_back(0, Command());
}

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, bool is_recovery)
    : bm_(bm), commit_idx(0), current_term_(0), max_inode_num(max_inode_num_), voted_for(-1)
{
    // Append an empty log entry at the beginning
    log_entries.emplace_back(0, Command());

    if (is_recovery) {
        auto res = FileOperation::create_from_raw(bm_);
        if (res.is_err()) {
            RAFT_FILE_OP_ERROR("Failed to create FileOperation from raw: %d", res.unwrap_error());
        }
        file_op_ = res.unwrap();
        return;
    }

    file_op_.reset(new FileOperation(bm_, max_inode_num));
    auto meta_res = file_op_->alloc_inode(InodeType::FILE);
    if (meta_res.is_err() || (meta_res.unwrap() != 1)) {
        RAFT_FILE_OP_ERROR("Init meta's file Error!");
    }
    auto log_res = file_op_->alloc_inode(InodeType::FILE);
    if (log_res.is_err() || (log_res.unwrap() != 2)) {
        RAFT_FILE_OP_ERROR("Init log's file Error!");
    }
    auto snapshot_res = file_op_->alloc_inode(InodeType::FILE);
    if (snapshot_res.is_err() || (snapshot_res.unwrap() != 3)) {
        RAFT_FILE_OP_ERROR("Init snapshot's file Error!");
    }
}

template <typename Command> RaftLog<Command>::~RaftLog()
{
    // Destructor implementation
}

template <typename Command> void RaftLog<Command>::append_entries(const std::vector<Command> &entries)
{
    std::unique_lock<std::mutex> lock(mtx);
    for (const auto &entry : entries) {
        log_entries.emplace_back(current_term_, entry);
    }
}

template <typename Command> int RaftLog<Command>::append_entry(int term, const Command &entry)
{
    std::unique_lock<std::mutex> lock(mtx);
    log_entries.emplace_back(term, entry);
    return log_entries.size() - 1;
}

template <typename Command> int RaftLog<Command>::append_entry(const RaftLogEntry<Command> &entry)
{
    std::unique_lock<std::mutex> lock(mtx);
    log_entries.push_back(entry);
    return log_entries.size() - 1;
}

template <typename Command> bool RaftLog<Command>::match_log(int index, int term) const
{
    std::unique_lock<std::mutex> lock(mtx);
    if (index < log_entries.size() && log_entries[index].term() == term) {
        return true;
    }
    return false;
}

template <typename Command> bool RaftLog<Command>::contain_index(int index) const
{
    std::unique_lock<std::mutex> lock(mtx);
    return index < log_entries.size();
}

template <typename Command> void RaftLog<Command>::truncate_log(int index)
{
    std::unique_lock<std::mutex> lock(mtx);
    if (index < log_entries.size()) {
        log_entries.resize(index);
    }
}

template <typename Command> int RaftLog<Command>::last_log_index() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return log_entries.size() - 1;
}

template <typename Command> int RaftLog<Command>::last_log_term() const
{
    std::unique_lock<std::mutex> lock(mtx);
    if (!log_entries.empty()) {
        return log_entries.back().term();
    }
    return 0;
}

template <typename Command> int RaftLog<Command>::commit_index() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return commit_idx;
}

template <typename Command> void RaftLog<Command>::set_commit_index(int index)
{
    std::unique_lock<std::mutex> lock(mtx);
    commit_idx = index;
}

template <typename Command> int RaftLog<Command>::term(int N) const
{
    std::unique_lock<std::mutex> lock(mtx);
    if (N < log_entries.size()) {
        return log_entries[N].term();
    }
    return 0;
}

template <typename Command> std::vector<u8> RaftLog<Command>::get_snapshot() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return node_snapshot;
}

template <typename Command> void RaftLog<Command>::set_snapshot(const std::vector<u8> &snapshot)
{
    std::unique_lock<std::mutex> lock(mtx);
    node_snapshot = snapshot;
}

template <typename Command> int RaftLog<Command>::get_snapshot_index() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return snapshot_index;
}

template <typename Command> void RaftLog<Command>::set_snapshot_index(int index)
{
    std::unique_lock<std::mutex> lock(mtx);
    snapshot_index = index;
}

template <typename Command> int RaftLog<Command>::get_snapshot_term() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return snapshot_term;
}

template <typename Command> void RaftLog<Command>::set_snapshot_term(int term)
{
    std::unique_lock<std::mutex> lock(mtx);
    snapshot_term = term;
}

template <typename Command> RaftLogEntry<Command> RaftLog<Command>::get_entry(int index) const
{
    std::unique_lock<std::mutex> lock(mtx);
    if (index < log_entries.size()) {
        return log_entries[index];
    }
    throw std::out_of_range("Index out of range with index: " + std::to_string(index) +
                            " and log size: " + std::to_string(log_entries.size()));
}

template <typename Command>
std::vector<RaftLogEntry<Command>> RaftLog<Command>::get_entries(int index, int length) const
{
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<RaftLogEntry<Command>> entries;
    for (int i = 0; i < length && index + i < log_entries.size(); i++) {
        entries.push_back(log_entries[index + i]);
    }
    return entries;
}

template <typename Command> int RaftLog<Command>::current_term() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return current_term_;
}

template <typename Command> void RaftLog<Command>::set_current_term(int term)
{
    std::unique_lock<std::mutex> lock(mtx);
    current_term_ = term;
}

template <typename Command> void RaftLog<Command>::set_voted_for(int candidate_id)
{
    std::unique_lock<std::mutex> lock(mtx);
    voted_for_ = candidate_id;
}

template <typename Command> int RaftLog<Command>::voted_for() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return voted_for_;
}

template <typename Command> void RaftLog<Command>::save_metadata() const
{
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<u8> data;
    data.push_back((current_term_ >> 24) & 0xff);
    data.push_back((current_term_ >> 16) & 0xff);
    data.push_back((current_term_ >> 8) & 0xff);
    data.push_back(current_term_ & 0xff);
    data.push_back((voted_for_ >> 24) & 0xff);
    data.push_back((voted_for_ >> 16) & 0xff);
    data.push_back((voted_for_ >> 8) & 0xff);
    data.push_back(voted_for_ & 0xff);
    return file_op_->write_file(metadata_inode, data);
}

template <typename Command> void RaftLog<Command>::save_log() const
{
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<u8> data;
    for (const auto &entry : log_entries) {
        data.push_back((entry.term() >> 24) & 0xff);
        data.push_back((entry.term() >> 16) & 0xff);
        data.push_back((entry.term() >> 8) & 0xff);
        data.push_back(entry.term() & 0xff);
        auto command_data = entry.command().serialize();
        data.insert(data.end(), command_data.begin(), command_data.end());
    }
    return file_op_->write_file(log_inode, data);
}

template <typename Command> void RaftLog<Command>::save_snapshot() const
{
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<u8> data;
    data.push_back((commit_idx >> 24) & 0xff);
    data.push_back((commit_idx >> 16) & 0xff);
    data.push_back((commit_idx >> 8) & 0xff);
    data.push_back(commit_idx & 0xff);
    return file_op_->write_file(snapshot_inode, data);
}

template <typename Command> void RaftLog<Command>::recover()
{
    std::unique_lock<std::mutex> lock(mtx);
    recover_metadata();
    recover_log();
    recover_snapshot();
}

template <typename Command> void RaftLog<Command>::recover_metadata()
{
    std::vector<u8> data;
    auto res = file_op_->read_file(metadata_inode);
    if (res.is_err()) {
        RAFT_FILE_OP_ERROR("Failed to read metadata: %d", res.unwrap_error());
    }
    data = res.unwrap();
    current_term_ = (data[0] << 24) | (data[1] << 16) | (data[2] << 8) | data[3];
    voted_for_ = (data[4] << 24) | (data[5] << 16) | (data[6] << 8) | data[7];
}

template <typename Command> void RaftLog<Command>::recover_log()
{
    log_entries.clear();
    std::vector<u8> data;
    auto res = file_op_->read_file(log_inode);
    if (res.is_err()) {
        RAFT_FILE_OP_ERROR("Failed to read log: %d", res.unwrap_error());
    }
    data = res.unwrap();
    int offset = 0;
    while (offset < data.size()) {
        int term = (data[offset] << 24) | (data[offset + 1] << 16) | (data[offset + 2] << 8) | data[offset + 3];
        offset += 4;
        Command command;
        int command_size = command.size();
        command.deserialize(std::vector<u8>(data.begin() + offset, data.begin() + offset + command_size), command_size);
        log_entries.emplace_back(term, command);
        offset += command_size;
    }

    snapshot_index = log_entries[0].te
}

template <typename Command> void RaftLog<Command>::recover_snapshot()
{
    std::vector<u8> data;
    auto res = file_op_->read_file(snapshot_inode);
    if (res.is_err()) {
        RAFT_FILE_OP_ERROR("Failed to read snapshot: %d", res.unwrap_error());
    }
    data = res.unwrap();
    node_snapshot = data;
}

} // namespace chfs