#pragma once

#include <cstdint>

namespace chfs
{

int election_timeout_base = 150;
int election_timeout_range = 150;
int run_background_election_sleep = 10;
int run_background_commit_sleep = 50;
int run_background_apply_sleep = 50;
int run_background_ping_sleep = 75; // half of minimum election timeout
int thread_pool_num = 16;
std::string block_file = "/tmp/block_file";

} // namespace chfs
