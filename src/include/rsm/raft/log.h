#pragma once

#include "block/manager.h"
#include "common/macros.h"
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

namespace chfs
{

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command> class RaftLog
{
  public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    // Append new entries to the log
    void append_entries(const std::vector<Command> &entries);

    // Check if the log contains an entry at the given index with the given term
    bool match_log(int index, int term) const;

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

  private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;
    std::vector<Command> log_entries; // Vector to store log entries
    int commit_idx;                   // Index of the highest log entry known to be committed
};

template <typename Command> RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm), commit_idx(0)
{
    // Constructor implementation
}

template <typename Command> RaftLog<Command>::~RaftLog()
{
    // Destructor implementation
}

template <typename Command> void RaftLog<Command>::append_entries(const std::vector<Command> &entries)
{
    // Empty implementation
}

template <typename Command> bool RaftLog<Command>::match_log(int index, int term) const
{
    // Empty implementation
    return false;
}

template <typename Command> void RaftLog<Command>::truncate_log(int index)
{
    // Empty implementation
}

template <typename Command> int RaftLog<Command>::last_log_index() const
{
    // Empty implementation
    return 0;
}

template <typename Command> int RaftLog<Command>::last_log_term() const
{
    // Empty implementation
    return 0;
}

template <typename Command> int RaftLog<Command>::commit_index() const
{
    // Empty implementation
    return commit_idx;
}

template <typename Command> void RaftLog<Command>::set_commit_index(int index)
{
    // Empty implementation
    commit_idx = index;
}

template <typename Command> int RaftLog<Command>::term(int N) const
{
    // Empty implementation
    return 0;
}

} // namespace chfs