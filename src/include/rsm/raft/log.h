#pragma once

#include "block/manager.h"
#include "common/macros.h"
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

  private:
    std::shared_ptr<BlockManager> bm_;
    mutable std::mutex mtx;
    std::vector<RaftLogEntry<Command>> log_entries; // Vector to store log entries
    int commit_idx;                                 // Index of the highest log entry known to be committed
    int current_term_;                              // Current term
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm), commit_idx(0), current_term_(0)
{
    // Append an empty log entry at the beginning
    log_entries.emplace_back(0, Command());
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

template <typename Command> void RaftLog<Command>::set_current_term(int term)
{
    std::unique_lock<std::mutex> lock(mtx);
    current_term_ = term;
}

template <typename Command> int RaftLog<Command>::current_term() const
{
    std::unique_lock<std::mutex> lock(mtx);
    return current_term_;
}

} // namespace chfs