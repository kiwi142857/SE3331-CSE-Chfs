#pragma once

#include <cstdint>

namespace chfs
{

int election_timeout_base = 150;
int election_timeout_range = 150;
int run_background_election_sleep = 10;
int heartbeat_interval = 75; // half of minimum election timeout
std::string block_file = "/tmp/block_file";

} // namespace chfs
