#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs
{

// TODO: this is a tmp global version id
version_t global_version_id = 0;

auto DataServer::initialize(std::string const &data_path)
{
    /**
     * At first check whether the file exists or not.
     * If so, which means the distributed chfs has
     * already been initialized and can be rebuilt from
     * existing data.
     */
    DEBUG_LOG("DataServer initializing...");
    bool is_initialized = is_file_exist(data_path);

    auto bm = std::shared_ptr<BlockManager>(new BlockManager(data_path, KDefaultBlockCnt));
    if (is_initialized) {
        DEBUG_LOG(data_path);
        DEBUG_LOG("DataServer is initialized from existing data.");
        block_allocator_ = std::make_shared<BlockAllocator>(bm, 0, false);
    } else {
        // We need to reserve some blocks for storing the version of each block
        block_allocator_ = std::shared_ptr<BlockAllocator>(new BlockAllocator(bm, 0, true));
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
    server_->bind("read_data", [this](block_id_t block_id, usize offset, usize len, version_t version) {
        return this->read_data(block_id, offset, len, version);
    });
    server_->bind("write_data", [this](block_id_t block_id, usize offset, std::vector<u8> &buffer) {
        return this->write_data(block_id, offset, buffer);
    });
    server_->bind("alloc_block", [this]() { return this->alloc_block(); });
    server_->bind("free_block", [this](block_id_t block_id) { return this->free_block(block_id); });

    // Launch the rpc server to listen for requests
    server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path) : server_(std::make_unique<RpcServer>(port))
{
    initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port))
{
    initialize(data_path);
}

DataServer::~DataServer()
{
    server_.reset();
}

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len, version_t version) -> std::vector<u8>
{
    // UNIMPLEMENTED();
    // 1. get the block from block_allocator_
    // 2. check the version of the block
    // 3. read the data from the block
    // 4. return the data

    // check version here
    // we'll read the version info in block[1], so we need to read the version info from block[1]
    // the data in block[1] is the version id, the k*sizeof(version_t) is the version id
    auto buf_version = new u8[block_allocator_->bm->block_size()];
    auto res_version = block_allocator_->bm->read_block(1, buf_version);
    if (res_version.is_err()) {
        return {};
    }
    version_t block_version = *reinterpret_cast<version_t *>(buf_version + block_id * sizeof(version_t));
    delete[] buf_version;

    // if the version is not equal to the global_version_id, we return an empty vector
    if (block_version != version) {
        DEBUG_LOG("Version mismatch: " << block_version << " vs " << version);
        return {};
    }

    auto buf = new u8[block_allocator_->bm->block_size()];
    auto res = block_allocator_->bm->read_block(block_id, buf);
    if (res.is_err()) {
        delete[] buf;
        return {};
    }
    std::vector<u8> data(buf + offset, buf + offset + len);
    delete[] buf;

    // check the version of the block
    // TODO: implement the version check here
    // We'll update the logic here to check the version of the block
    // now we will direct return the data
    return data;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset, std::vector<u8> &buffer) -> bool
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // 2. check the version of the block
    // 3. write the data to the block
    // 4. return true if success

    // TODO: implement the version check here
    // we'll update the logic of version check here

    // we call write_partial_block to write the data
    auto res = block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
    if (res.is_err()) {
        return false;
    }
    return true;

    // return false;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // TODO: implement the version check here
    // we'll update the logic of version check here

    // we call allocate to allocate a block
    auto res = block_allocator_->allocate();
    if (res.is_err()) {
        return {};
    }

    // update the version id, we store the version info in block[1], so we need to update the version id, we keep the
    // version id in the global_version_id the data in block[1] is the version id, the k*sizeof(version_t) is the
    // version id
    global_version_id++;
    // write the version id to the block[1]
    block_id_t block_id = res.unwrap();
    auto res_write = block_allocator_->bm->write_partial_block(1, reinterpret_cast<const u8 *>(&global_version_id),
                                                               block_id * sizeof(version_t), sizeof(version_t));
    if (res_write.is_err()) {
        return {};
    }

    DEBUG_LOG("Allocated block id: " << res.unwrap());
    return {res.unwrap(), global_version_id};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // TODO: implement the version check here
    // we'll update the logic of version check here
    global_version_id++;

    // we call deallocate to deallocate a block
    auto res = block_allocator_->deallocate(block_id);
    if (res.is_err()) {
        return false;
    }

    // update the version id, we store the version info in block[1], so we need to update the version id, we keep the
    // version id in the global_version_id the data in block[1] is the version id, the k*sizeof(version_t) is the
    // version id
    auto res_write = block_allocator_->bm->write_partial_block(1, reinterpret_cast<const u8 *>(&global_version_id),
                                                               block_id * sizeof(version_t), sizeof(version_t));
    if (res_write.is_err()) {
        return false;
    }

    return true;
}
} // namespace chfs