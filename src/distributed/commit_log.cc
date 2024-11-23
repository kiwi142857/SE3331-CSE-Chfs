#include <algorithm>

#include "common/bitmap.h"
#include "common/config.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs
{
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm, bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm), current_offset(0), tx_num(0)
{
}

CommitLog::~CommitLog()
{
}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    return tx_num;
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id, std::vector<std::shared_ptr<BlockOperation>> ops) -> void
{
    std::lock_guard<std::mutex> lock(log_mtx);
    // TODO: Implement this function.
    auto initial_offset = current_offset.load();
    // traverse all the ops
    for (auto &op : ops) {
        LogEntry log_entry(txn_id, op->block_id_);
        std::vector<u8> buffer(sizeof(LogEntry) + DiskBlockSize);
        log_entry.flush_to_buffer(buffer.data());
        auto log_entry_ptr = reinterpret_cast<LogEntry *>(buffer.data());
        for (int i = 0; i < DiskBlockSize; ++i) {
            log_entry_ptr->new_block_state[i] = op->new_block_state_[i];
        }
        // write the log entry to the log
        bm_->write_log_entry(current_offset, buffer.data(), sizeof(LogEntry) + DiskBlockSize);
        current_offset += sizeof(LogEntry) + DiskBlockSize;
    }
    // persist the log
    const auto BLOCK_SIZE = bm_->block_size();
    auto start_block_id = initial_offset / BLOCK_SIZE;
    auto end_block_id = current_offset / BLOCK_SIZE;
    const auto base_block_id = bm_->total_blocks();
    for (auto i = start_block_id; i < end_block_id; ++i) {
        bm_->sync(i + base_block_id);
    }

    commit_log(txn_id);

    if (is_checkpoint_enabled_) {
        // checkpoint
        if (tx_num % 20 == 0 || current_offset >= DiskBlockSize * 1000) {
            checkpoint();
        }
    }
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto initial_offset = current_offset.load();
    LogEntry log_entry(txn_id, commit_marker);
    std::vector<u8> buffer(sizeof(LogEntry) + DiskBlockSize);
    log_entry.flush_to_buffer(buffer.data());
    bm_->write_log_entry(current_offset, buffer.data(), sizeof(LogEntry) + DiskBlockSize);
    current_offset += sizeof(LogEntry) + DiskBlockSize;

    // persist the log
    const auto BLOCK_SIZE = bm_->block_size();
    auto start_block_id = initial_offset / BLOCK_SIZE;
    auto end_block_id = current_offset / BLOCK_SIZE;
    const auto base_block_id = bm_->total_blocks();
    for (auto i = start_block_id; i < end_block_id; ++i) {
        bm_->sync(i + base_block_id);
    }
}

// {Your code here}
auto CommitLog::checkpoint() -> void
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto log_start = bm_->get_log_start();
    memset(log_start, 0, DiskBlockSize * KCommitLogNum);
    current_offset = 0;
    tx_num = 0;

    // persist the log
    bm_->flush();
}

// {Your code here}
auto CommitLog::recover() -> void
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    const auto log_start = bm_->get_log_start();
    const auto log_end = bm_->get_log_end();
    auto it = log_start;
    while (it < log_end) {
        auto log_entry_ptr = reinterpret_cast<LogEntry *>(it);
        if (log_entry_ptr->txn_id == 0) {
            break;
        }
        // num_entry_tx will record how many entry is belong to this transaction
        auto num_entry_tx = 0;
        auto this_txn_id = log_entry_ptr->txn_id;
        bool is_this_tx_committed = false;
        auto txn_it = it;
        while (txn_it < log_end) {
            num_entry_tx++;
            auto log_entry_ptr = reinterpret_cast<LogEntry *>(txn_it);
            if (log_entry_ptr->txn_id != this_txn_id) {
                break;
            }
            if (log_entry_ptr->block_id == commit_marker) {
                is_this_tx_committed = true;
                break;
            }
            txn_it += (sizeof(LogEntry) + DiskBlockSize);
        }
        if (!is_this_tx_committed) {
            it += (sizeof(LogEntry) + DiskBlockSize) * num_entry_tx;
            continue;
        }

        // redo the operation
        auto redo_it = it;
        while (redo_it < txn_it) {
            auto log_entry_ptr = reinterpret_cast<LogEntry *>(redo_it);
            auto block_id = log_entry_ptr->block_id;
            auto new_block_state = log_entry_ptr->new_block_state;

            bm_->write_block_for_recover(block_id, new_block_state.data());

            redo_it += (sizeof(LogEntry) + DiskBlockSize);
        }
        it += (sizeof(LogEntry) + DiskBlockSize) * num_entry_tx;
    }
}
}; // namespace chfs