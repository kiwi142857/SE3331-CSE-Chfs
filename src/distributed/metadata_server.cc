#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs
{

inline auto MetadataServer::bind_handlers()
{
    server_->bind("mknode", [this](u8 type, inode_id_t parent, std::string const &name) {
        return this->mknode(type, parent, name);
    });
    server_->bind("unlink", [this](inode_id_t parent, std::string const &name) { return this->unlink(parent, name); });
    server_->bind("lookup", [this](inode_id_t parent, std::string const &name) { return this->lookup(parent, name); });
    server_->bind("get_block_map", [this](inode_id_t id) { return this->get_block_map(id); });
    server_->bind("alloc_block", [this](inode_id_t id) { return this->allocate_block(id); });
    server_->bind("free_block", [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
        return this->free_block(id, block, machine_id);
    });
    server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
    server_->bind("get_type_attr", [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path)
{
    /**
     * Check whether the metadata exists or not.
     * If exists, we wouldn't create one from scratch.
     */
    bool is_initialed = is_file_exist(data_path);

    auto block_manager = std::shared_ptr<BlockManager>(nullptr);
    if (is_log_enabled_) {
        DEBUG_LOG("MetadataServer initializing... from " << data_path);
        block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
    } else {
        block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
    }

    CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

    if (is_initialed) {
        auto origin_res = FileOperation::create_from_raw(block_manager);
        std::cout << "Restarting..." << std::endl;
        if (origin_res.is_err()) {
            std::cerr << "Original FS is bad, please remove files manually." << std::endl;
            exit(1);
        }

        operation_ = origin_res.unwrap();
    } else {
        operation_ = std::make_shared<FileOperation>(block_manager, DistributedMaxInodeSupported);
        std::cout << "We should init one new FS..." << std::endl;
        /**
         * If the filesystem on metadata server is not initialized, create
         * a root directory.
         */
        auto init_res = operation_->alloc_inode(InodeType::Directory);
        if (init_res.is_err()) {
            std::cerr << "Cannot allocate inode for root directory." << std::endl;
            exit(1);
        }

        CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
    }

    running = false;
    num_data_servers = 0; // Default no data server. Need to call `reg_server` to add.

    if (is_log_enabled_) {
        if (may_failed_)
            operation_->block_manager_->set_may_fail(true);
        commit_log = std::make_shared<CommitLog>(operation_->block_manager_, is_checkpoint_enabled_);
    }

    bind_handlers();

    /**
     * The metadata server wouldn't start immediately after construction.
     * It should be launched after all the data servers are registered.
     */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path, bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed), is_checkpoint_enabled_(is_checkpoint_enabled)
{
    server_ = std::make_unique<RpcServer>(port);
    init_fs(data_path);
    if (is_log_enabled_) {
        commit_log = std::make_shared<CommitLog>(operation_->block_manager_, is_checkpoint_enabled);
    }
}

MetadataServer::MetadataServer(std::string const &address, u16 port, const std::string &data_path, bool is_log_enabled,
                               bool is_checkpoint_enabled, bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed), is_checkpoint_enabled_(is_checkpoint_enabled)
{
    server_ = std::make_unique<RpcServer>(address, port);
    init_fs(data_path);
    if (is_log_enabled_) {
        commit_log = std::make_shared<CommitLog>(operation_->block_manager_, is_checkpoint_enabled);
    }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name) -> inode_id_t
{

    // 1. Allocate an inode
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // TODO: if we don't enable log and transaction feature
    if (type == DirectoryType) {
        std::lock_guard<std::mutex> lock(inode_mutex_);
        auto mkdir_res = operation_->mkdir(parent, name.data());
        if (mkdir_res.is_err()) {
            DEBUG_LOG("mkdir failed");
            return KInvalidInodeID;
        }
        DEBUG_LOG("mkdir success" << mkdir_res.unwrap());
        return mkdir_res.unwrap();
    } else if (type == RegularFileType) {
        std::lock_guard<std::mutex> lock(inode_mutex_);
        auto mkfile_res = operation_->mkfile(parent, name.data());
        if (mkfile_res.is_err()) {
            DEBUG_LOG("mkfile failed");
            return KInvalidInodeID;
        }
        // We print the file created here with the filename and the inode id
        DEBUG_LOG(name << " created with inode id: " << mkfile_res.unwrap());
        DEBUG_LOG("mkfile success");
        return mkfile_res.unwrap();
    } else {
        return KInvalidInodeID;
    }

    return KInvalidInodeID;
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name) -> bool
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // TODO: if we don't enable log and transaction feature
    std::lock_guard<std::mutex> lock(inode_mutex_);
    auto lookup_result = lookup(parent, name);
    if (lookup_result == KInvalidInodeID) {
        return false;
    }

    // To unlink a file, we need to remove the file and remove the entry from the directory and update its inode and
    // block bitmap
    // 1. Call data server to free the block
    // 2. Call ‘remove_metaserver_inode’ to remove the inode from the metadata server

    // 1. we will call the free_block to free the block
    auto block_map = get_block_map(lookup_result);
    for (const auto &block_info : block_map) {
        auto cli = clients_[std::get<1>(block_info)];
        auto response = cli->call("free_block", std::get<0>(block_info));
        if (response.is_err()) {
            return false;
        }
        if (!response.unwrap()->as<bool>()) {
            return false;
        }
    }

    // 2. we will call the remove_metaserver_inode to remove the inode from the metadata server
    auto remove_res = operation_->remove_metaserver_inode(parent, name.data());
    if (remove_res.is_err()) {
        return false;
    }

    return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name) -> inode_id_t
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    DEBUG_LOG("lookup: " << name);
    auto lookup_res = operation_->lookup(parent, name.data());
    if (lookup_res.is_err()) {
        DEBUG_LOG("lookup failed");
        return KInvalidInodeID;
    }
    DEBUG_LOG("lookup success: " << lookup_res.unwrap());
    return lookup_res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // 1. Get the inode
    // 2. Return the block map

    // get the inode and its corresponding block, we will return the block info
    std::vector<BlockInfo> block_infos;
    auto get_res = operation_->inode_manager_->get(id);
    if (get_res.is_err()) {
        return block_infos;
    }
    auto inode_block_id = get_res.unwrap();
    if (inode_block_id == KInvalidBlockID) {
        return block_infos;
    }
    std::vector<u8> file_inode(operation_->block_manager_->block_size());
    auto read_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
    if (read_res.is_err()) {
        return block_infos;
    }
    auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
    for (uint i = 0; i < inode_ptr->get_nblocks(); i = i + 2) {
        if (inode_ptr->blocks[i] == KInvalidBlockID) {
            break;
        }
        // TODO: we need to get the version of the block
        // block_infos.push_back(BlockInfo(inode_ptr->blocks[i], inode_ptr->blocks[i + 1], 0));
        // We call the read_data to the macid to get block[1] to get the version of the block
        auto mac_id = inode_ptr->blocks[i + 1];
        auto cli = clients_[mac_id];
        auto response = cli->call("read_data", 1, inode_ptr->blocks[i] * sizeof(version_t), sizeof(version_t), 0);
        if (response.is_err()) {
            return block_infos;
        }

        auto obj_handle = response.unwrap();
        auto response_vec = response.unwrap()->as<std::vector<u8>>();
        auto version_ptr = reinterpret_cast<version_t *>(response_vec.data());
        //...fetch version finish...//
        version_t version_id = *version_ptr;
        block_infos.push_back(BlockInfo(inode_ptr->blocks[i], mac_id, version_id));
    }
    return block_infos;
    return {};
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // 1. Allocate a block
    // 2. Record the block in the inode
    // 3. Return the block info

    // first, we will check whether the inode block have enough space to store a new block info pair
    const auto BLOCK_SIZE = operation_->block_manager_->block_size();
    usize old_block_num = 0;
    // u64 original_file_sz = 0;

    // 加锁
    std::lock_guard<std::mutex> lock(inode_mutex_);
    // 1. read the inode
    std::vector<u8> file_inode(BLOCK_SIZE);
    auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
    auto get_res = operation_->inode_manager_->get(id);
    auto inode_block_id = get_res.unwrap();
    if (inode_block_id == KInvalidBlockID) {
        return BlockInfo(KInvalidBlockID, 0, 0);
    }
    auto read_block_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
    if (read_block_res.is_err()) {
        return BlockInfo(KInvalidBlockID, 0, 0);
    }
    // 2. make sure that we have space to allocate a new block for this file
    // it seems like it just only alloc not write, so this operation won't change attr
    // there are two methods to get old_block_num
    // (1) by file size
    // original_file_sz = inode_ptr->get_size();
    // old_block_num = (original_file_sz % BLOCK_SIZE) ? (original_file_sz / BLOCK_SIZE + 1) : (original_file_sz /
    // BLOCK_SIZE); (2) by first Invalid slot
    old_block_num = 0;
    for (uint i = 0; i < inode_ptr->get_nblocks(); i = i + 2) {
        if (inode_ptr->blocks[i] == KInvalidBlockID) {
            old_block_num = i / 2;
            break;
        }
    }

    // check after alloc the Inode block is whether full or not
    if (2 * (old_block_num + 1) > inode_ptr->get_nblocks()) {
        return BlockInfo(KInvalidBlockID, 0, 0);
    }
    auto idx = 2 * old_block_num;
    // prepare work finish

    // second, we randomly alloc a block from dataservers
    std::vector<mac_id_t> mac_ids;
    for (const auto &pair : clients_) {
        mac_ids.push_back(pair.first);
    }
    //! notice: this rand function is not as usually closed on the left, open on the right([a, b)), this is a both
    //! closed [a,b]
    auto rand_num = generator.rand(0, mac_ids.size() - 1);
    mac_id_t target_mac_id = mac_ids[rand_num];
    auto it = clients_.find(target_mac_id);
    if (it == clients_.end()) {
        return BlockInfo(KInvalidBlockID, 0, 0);
    }
    auto target_mac = it->second;
    auto response = target_mac->call("alloc_block");
    if (response.is_err()) {
        return BlockInfo(KInvalidBlockID, target_mac_id, 0);
    }
    auto response_pair = response.unwrap()->as<std::pair<chfs::block_id_t, chfs::version_t>>();
    auto block_id = response_pair.first;
    auto version_id = response_pair.second;

    // third, update the info in metadata_server locally
    inode_ptr->set_block_direct(idx, block_id);
    inode_ptr->set_block_direct(idx + 1, target_mac_id);

    // maybe todo: update the attr of inode

    auto write_res = operation_->block_manager_->write_block(inode_block_id, file_inode.data());
    if (write_res.is_err()) {
        return BlockInfo(KInvalidBlockID, 0, 0);
    }
    return BlockInfo(block_id, target_mac_id, version_id);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id, mac_id_t machine_id) -> bool
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    if (id > operation_->inode_manager_->get_max_inode_supported()) {
        return false;
    }
    if (block_id == KInvalidBlockID) {
        return false;
    }

    // 1. Remove the block from the inode
    // 2. Send a free block request to the data server
    // 3. Return the result

    // first, we will check whether the block is in the inode block or not, also we will check if the mac id is correct
    // and get the index

    const auto BLOCK_SIZE = operation_->block_manager_->block_size();

    std::lock_guard<std::mutex> lock(inode_mutex_);
    // 1. read the inode
    std::vector<u8> file_inode(BLOCK_SIZE);
    auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
    auto get_res = operation_->inode_manager_->get(id);
    auto inode_block_id = get_res.unwrap();
    if (inode_block_id == KInvalidBlockID) {
        return false;
    }
    auto read_block_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
    if (read_block_res.is_err()) {
        return false;
    }

    bool is_found = false;
    uint record_idx = 0;
    for (uint i = 0; i < inode_ptr->get_nblocks(); i = i + 2) {
        if (inode_ptr->blocks[i] == block_id && inode_ptr->blocks[i + 1] == machine_id) {
            is_found = true;
            record_idx = i;
            break;
        }
    }
    if (!is_found) {
        return false;
    }

    // 2. send a free block request to the data server
    auto it = clients_.find(machine_id);
    if (it == clients_.end()) {
        return false;
    }
    auto target_mac = it->second;
    auto response = target_mac->call("free_block", block_id);
    if (response.is_err()) {
        return false;
    }
    auto response_bool = response.unwrap()->as<bool>();
    if (!response_bool) {
        return false;
    }

    // 3. remove the block from the inode
    for (uint i = record_idx; i < inode_ptr->get_nblocks(); i = i + 2) {
        if (i + 3 >= inode_ptr->get_nblocks()) {
            inode_ptr->blocks[i] = KInvalidBlockID;
            inode_ptr->blocks[i + 1] = 0;
            break;
        }
        inode_ptr->blocks[i] = inode_ptr->blocks[i + 2];
        inode_ptr->blocks[i + 1] = inode_ptr->blocks[i + 3];
        if (inode_ptr->blocks[i + 2] == KInvalidBlockID) {
            break;
        }
    }

    // set size
    inode_ptr->set_size(inode_ptr->get_size() - BLOCK_SIZE);
    auto write_res = operation_->block_manager_->write_block(inode_block_id, file_inode.data());
    if (write_res.is_err()) {
        return false;
    }
    return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node) -> std::vector<std::pair<std::string, inode_id_t>>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    std::vector<std::pair<std::string, inode_id_t>> res_vec(0);
    std::list<DirectoryEntry> list;
    auto read_res = read_directory(operation_.get(), node, list);
    if (read_res.is_err()) {
        return res_vec;
    }
    for (const auto &entry : list) {
        std::pair<std::string, inode_id_t> vec_element(entry.name, entry.id);
        res_vec.push_back(vec_element);
    }
    return res_vec;

    return {};
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id) -> std::tuple<u64, u64, u64, u64, u8>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // 1. Get the inode
    // 2. Return the type and attribute

    auto get_res = operation_->inode_manager_->get(id);
    if (get_res.is_err()) {
        return std::make_tuple(0, 0, 0, 0, 0);
    }
    auto inode_block_id = get_res.unwrap();
    if (inode_block_id == KInvalidBlockID) {
        return std::make_tuple(0, 0, 0, 0, 0);
    }
    std::vector<u8> file_inode(operation_->block_manager_->block_size());
    auto read_res = operation_->block_manager_->read_block(inode_block_id, file_inode.data());
    if (read_res.is_err()) {
        return std::make_tuple(0, 0, 0, 0, 0);
    }
    auto inode_ptr = reinterpret_cast<Inode *>(file_inode.data());
    FileAttr attr = inode_ptr->get_attr();
    return std::make_tuple(inode_ptr->get_size(), attr.atime, attr.mtime, attr.ctime,
                           static_cast<u8>(inode_ptr->get_type()));
}

auto MetadataServer::reg_server(const std::string &address, u16 port, bool reliable) -> bool
{
    num_data_servers += 1;
    auto cli = std::make_shared<RpcClient>(address, port, reliable);
    clients_.insert(std::make_pair(num_data_servers, cli));

    return true;
}

auto MetadataServer::run() -> bool
{
    if (running)
        return false;

    // Currently we only support async start
    server_->run(true, num_worker_threads);
    running = true;
    return true;
}

} // namespace chfs