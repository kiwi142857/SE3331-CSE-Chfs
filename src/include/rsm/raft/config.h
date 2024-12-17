#pragma once

#include <cstdint>
#include <string>

namespace chfs
{

const int election_timeout_base = 150;
const int election_timeout_range = 150;
const int run_background_election_sleep = 10;
const int run_background_commit_sleep = 50;
const int run_background_apply_sleep = 50;
const int run_background_ping_sleep = 75; // half of minimum election timeout
const int thread_pool_size = 16;
const int max_inode_num_ = 16;
const int metadata_inode = 1;
const int log_inode = 2;
const int snapshot_inode = 3;
const std::string raft_log_folder = "/tmp/raft_log";
const std::string node_file_subpath = "/node_";

} // namespace chfs
