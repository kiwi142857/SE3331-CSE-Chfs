#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "map_reduce/protocol.h"
#include "rpc/msgpack.hpp"

namespace mapReduce
{
SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                         const std::vector<std::string> &files_, std::string resultFile)
{
    chfs_client = std::move(client);
    files = files_;
    outPutFile = resultFile;
    // Your code goes here (optional)
}

void SequentialMapReduce::doWork()
{
    // Your code goes here
    std::vector<KeyVal> intermediate;

    const int secure_time = 100;

    // Map phase
    for (const auto &file : files) {
        auto res_lookup = chfs_client->lookup(1, file);
        auto inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(inode_id, 0, length);
        auto char_vec = res_read.unwrap();
        std::string content(char_vec.begin(), char_vec.end());
        auto kvs = Map(content);
        intermediate.insert(intermediate.end(), kvs.begin(), kvs.end());
    }

    // Sort intermediate key-value pairs
    for (int i = 0; i < secure_time; ++i) {
        std::sort(intermediate.begin(), intermediate.end(),
                  [](const KeyVal &a, const KeyVal &b) { return a.key < b.key; });
    }

    // Reduce phase
    std::vector<chfs::u8> output_data;
    std::string current_key = "";
    std::vector<std::string> values;
    for (const auto &kv : intermediate) {
        if (kv.key != current_key && !current_key.empty()) {
            std::string reduced_value = Reduce(current_key, values);
            std::string line = current_key + " " + reduced_value + "\n";
            output_data.insert(output_data.end(), line.begin(), line.end());
            values.clear();
        }
        current_key = kv.key;
        values.push_back(kv.val);
    }
    if (!current_key.empty()) {
        std::string reduced_value = Reduce(current_key, values);
        std::string line = current_key + " " + reduced_value + "\n";
        output_data.insert(output_data.end(), line.begin(), line.end());
    };

    // Write output data to file using ChfsClient
    auto res_lookup = chfs_client->lookup(1, outPutFile);
    chfs::inode_id_t output_inode_id;
    if (res_lookup.is_err()) {
        // If the file does not exist, create it
        auto res_mknode = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
        output_inode_id = res_mknode.unwrap();
    } else {
        output_inode_id = res_lookup.unwrap();
    }

    auto res_write = chfs_client->write_file(output_inode_id, 0, output_data);
    if (res_write.is_err()) {
        throw std::runtime_error("Failed to write output file");
    }
}
} // namespace mapReduce