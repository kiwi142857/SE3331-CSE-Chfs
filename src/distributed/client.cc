#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs
{

ChfsClient::ChfsClient() : num_data_servers(0)
{
}

auto ChfsClient::reg_server(ServerType type, const std::string &address, u16 port, bool reliable) -> ChfsNullResult
{
    switch (type) {
    case ServerType::DATA_SERVER:
        num_data_servers += 1;
        data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(address, port, reliable)});
        break;
    case ServerType::METADATA_SERVER:
        metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
        break;
    default:
        std::cerr << "Unknown Type" << std::endl;
        exit(1);
    }

    return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent, const std::string &name) -> ChfsResult<inode_id_t>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();

    // 1. ask metadata server to create a new node
    auto res = metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
    if (res.is_err()) {
        return ChfsResult<inode_id_t>(res.unwrap_error());
    }
    auto inode_id = res.unwrap()->as<inode_id_t>();
    if (inode_id == KInvalidInodeID) {
        return ChfsResult<inode_id_t>(ErrorType::INVALID);
    }

    return ChfsResult<inode_id_t>(inode_id);

    // return ChfsResult<inode_id_t>(0);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name) -> ChfsNullResult
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = metadata_server_->call("unlink", parent, name);
    if (res.is_err()) {
        return res.unwrap_error();
    }
    if (!res.unwrap()->as<bool>()) {
        return ErrorType::INVALID;
    }
    return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name) -> ChfsResult<inode_id_t>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    DEBUG_LOG("call Look UP here");
    auto res = metadata_server_->call("lookup", parent, name);
    if (res.is_err()) {
        DEBUG_LOG("lookup error");
        return ChfsResult<inode_id_t>(res.unwrap_error());
    }
    DEBUG_LOG("lookup step1");
    auto inode_id = res.unwrap()->as<inode_id_t>();
    DEBUG_LOG("lookup step2");
    if (inode_id == KInvalidInodeID) {
        DEBUG_LOG("lookup invalid inode id");
        return ChfsResult<inode_id_t>(ErrorType::NotExist);
    }
    DEBUG_LOG("lookup step3, inode_id: " << inode_id);

    return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id) -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = metadata_server_->call("readdir", id);
    if (res.is_err()) {
        return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(res.unwrap_error());
    }
    auto entries = res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
    if (entries.empty()) {
        return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(ErrorType::INVALID);
    }
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(entries);
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id) -> ChfsResult<std::pair<InodeType, FileAttr>>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto res = metadata_server_->call("get_type_attr", id);
    if (res.is_err()) {
        return ChfsResult<std::pair<InodeType, FileAttr>>(res.unwrap_error());
    }
    auto type_attr = res.unwrap()->as<std::tuple<u64, u64, u64, u64, u64>>();
    auto type = static_cast<InodeType>(std::get<4>(type_attr));
    FileAttr attr;
    attr.size = std::get<0>(type_attr);
    attr.atime = std::get<1>(type_attr);
    attr.mtime = std::get<2>(type_attr);
    attr.ctime = std::get<3>(type_attr);

    DEBUG_LOG("get file size: " << attr.size);

    return ChfsResult<std::pair<InodeType, FileAttr>>(std::make_pair(type, attr));
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size) -> ChfsResult<std::vector<u8>>
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    DEBUG_LOG("Read file id: " << id << " offset: " << offset << " size: " << size);
    // The client go to the metadata server to get the block map of the file
    auto block_infos_res = metadata_server_->call("get_block_map", id);
    DEBUG_LOG("Read file step2 ");
    if (block_infos_res.is_err()) {
        ERROR_LOG("Get block map failed");
        return ChfsResult<std::vector<u8>>(block_infos_res.unwrap_error());
    }
    auto block_infos = block_infos_res.unwrap()->as<std::vector<BlockInfo>>();
    if (block_infos.empty()) {
        ERROR_LOG("Block info is empty");
        return ChfsResult<std::vector<u8>>(ErrorType::INVALID);
    }
    const auto BLOCK_SIZE = DiskBlockSize;
    // The client goes to the data server to read the data
    std::vector<u8> data;
    for (auto &block_info : block_infos) {
        auto mac_id = std::get<1>(block_info);
        auto cli = data_servers_[mac_id];

        DEBUG_LOG("Read data from block id: " << std::get<0>(block_info));
        auto response = cli->call("read_data", std::get<0>(block_info), 0, BLOCK_SIZE, std::get<2>(block_info));
        if (response.is_err()) {
            ERROR_LOG("Read data failed");
            return ChfsResult<std::vector<u8>>(response.unwrap_error());
        }
        auto block_data = response.unwrap()->as<std::vector<u8>>();
        data.insert(data.end(), block_data.begin(), block_data.end());
    }
    if (data.size() < offset + size) {
        return ChfsResult<std::vector<u8>>(ErrorType::INVALID);
    }
    return ChfsResult<std::vector<u8>>(std::vector<u8>(data.begin() + offset, data.begin() + offset + size));
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data) -> ChfsNullResult
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    DEBUG_LOG("Write file id: " << id << " offset: " << offset << " size: " << data.size());

    const auto BLOCK_SIZE = DiskBlockSize;
    auto write_length = data.size();
    auto get_block_map_response = metadata_server_->call("get_block_map", id);
    if (get_block_map_response.is_err()) {
        auto error_code = get_block_map_response.unwrap_error();
        return ChfsNullResult(error_code);
    }
    auto block_info_vec = get_block_map_response.unwrap()->as<std::vector<chfs::BlockInfo>>();
    auto old_file_sz = block_info_vec.size() * BLOCK_SIZE;

    if (offset + write_length > old_file_sz) {
        auto new_block_num = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) / BLOCK_SIZE + 1)
                                                                    : ((offset + write_length) / BLOCK_SIZE);
        auto old_block_num = block_info_vec.size();
        for (auto i = old_block_num; i < new_block_num; ++i) {
            auto alloc_response = metadata_server_->call("alloc_block", id);
            if (alloc_response.is_err()) {
                auto error_code = alloc_response.unwrap_error();
                return ChfsNullResult(error_code);
            }
            auto new_block_info = alloc_response.unwrap()->as<BlockInfo>();
            block_info_vec.push_back(new_block_info);
        }
    }

    auto write_start_idx = offset / BLOCK_SIZE;
    auto write_start_offset = offset % BLOCK_SIZE;
    auto write_end_idx = ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) / BLOCK_SIZE + 1)
                                                                : ((offset + write_length) / BLOCK_SIZE);
    auto write_end_offset =
        ((offset + write_length) % BLOCK_SIZE) ? ((offset + write_length) % BLOCK_SIZE) : BLOCK_SIZE;

    usize current_offset = 0;
    for (auto it = block_info_vec.begin() + write_start_idx; it != block_info_vec.begin() + write_end_idx; ++it) {
        block_id_t block_id = std::get<0>(*it);
        mac_id_t mac_id = std::get<1>(*it);
        auto mac_it = data_servers_.find(mac_id);
        if (mac_it == data_servers_.end()) {
            auto error_code = ErrorType::INVALID_ARG;
            return ChfsNullResult(error_code);
        }
        auto target_mac = mac_it->second;
        std::vector<u8> write_buf;
        usize per_write_offset = 0;
        if (it == block_info_vec.begin() + write_start_idx && it == block_info_vec.begin() + (write_end_idx - 1)) {
            auto write_response = target_mac->call("write_data", block_id, write_start_offset, data);
            if (write_response.is_err()) {
                auto error_code = write_response.unwrap_error();
                return ChfsNullResult(error_code);
            }
            auto is_success = write_response.unwrap()->as<bool>();
            if (!is_success) {
                auto error_code = ErrorType::INVALID;
                return ChfsNullResult(error_code);
            }
            return KNullOk;
        }
        if (it == block_info_vec.begin() + write_start_idx) {
            write_buf.resize(BLOCK_SIZE - write_start_offset);
            std::copy_n(data.begin(), BLOCK_SIZE - write_start_offset, write_buf.begin());
            per_write_offset = write_start_offset;
            current_offset += BLOCK_SIZE - write_start_offset;
        } else if (it == block_info_vec.begin() + (write_end_idx - 1)) {
            write_buf.resize(write_end_offset);
            std::copy_n(data.begin() + current_offset, write_end_offset, write_buf.begin());
            per_write_offset = 0;
            current_offset += write_end_offset;
        } else {
            write_buf.resize(BLOCK_SIZE);
            std::copy_n(data.begin() + current_offset, BLOCK_SIZE, write_buf.begin());
            per_write_offset = 0;
            current_offset += BLOCK_SIZE;
        }
        auto write_response = target_mac->call("write_data", block_id, per_write_offset, write_buf);
        if (write_response.is_err()) {
            auto error_code = write_response.unwrap_error();
            return ChfsNullResult(error_code);
        }
        auto is_success = write_response.unwrap()->as<bool>();
        if (!is_success) {
            auto error_code = ErrorType::INVALID;
            return ChfsNullResult(error_code);
        }
    }

    return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id, mac_id_t mac_id) -> ChfsNullResult
{
    // TODO: Implement this function.
    // UNIMPLEMENTED();
    auto response = metadata_server_->call("free_block", id, block_id, mac_id);
    if (response.is_err()) {
        auto error_code = response.unwrap_error();
        return ChfsNullResult(error_code);
    }
    auto is_success = response.unwrap()->as<bool>();
    if (!is_success) {
        auto error_code = ErrorType::NotExist;
        return ChfsNullResult(error_code);
    }
    return KNullOk;
}

} // namespace chfs