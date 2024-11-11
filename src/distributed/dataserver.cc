#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  DEBUG_LOG("DataServer initializing...");
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    DEBUG_LOG("DataServer is initialized from existing data.");
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
    // allocate the first block for storing the version of each block
    auto res = block_allocator_->allocate();
    if (res.is_err()) {
      throw std::runtime_error("Failed to allocate the first block");
    }
    // print the id of the first block
    block_id_t version_block_id = res.unwrap();
    DEBUG_LOG("Version block id: " << version_block_id);
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // UNIMPLEMENTED();
  // 1. get the block from block_allocator_
  // 2. check the version of the block
  // 3. read the data from the block
  // 4. return the data
  auto buf = new u8[block_allocator_->bm->block_size()];
  auto res = block_allocator_->bm->read_block(block_id, buf);
  if (res.is_err()) {
    delete[] buf;
    return {};
  }
  std::vector<u8> data(buf + offset, buf + offset + len);
  delete[] buf;

  // check the version of the block


  return {};
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();

  return false;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  // UNIMPLEMENTED();

  return {};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  // UNIMPLEMENTED();

  return false;
}
} // namespace chfs