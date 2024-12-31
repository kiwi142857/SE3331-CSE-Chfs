#include <fstream>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "common/config.h"
#include "map_reduce/protocol.h"
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace mapReduce
{

Worker::Worker(MR_CoordinatorConfig config)
{
    mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
    outPutFile = config.resultFile;
    chfs_client = config.client;
    work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
    // Lab4: Your code goes here (Optional).
    nReduce = chfs::KReduce;
}

void Worker::doMap(int index, const std::string &filename)
{
    auto res_lookup = chfs_client->lookup(1, filename);
    auto inode_id = res_lookup.unwrap();
    auto res_type = chfs_client->get_type_attr(inode_id);
    auto length = res_type.unwrap().second.size;
    auto res_read = chfs_client->read_file(inode_id, 0, length);
    auto char_vec = res_read.unwrap();
    std::string content(char_vec.begin(), char_vec.end());
    auto kvs = Map(content);

    std::string intermediate_filename = "mr-" + std::to_string(index);
    std::vector<chfs::u8> output_data;
    for (const auto &kv : kvs) {
        std::string line = kv.key + " " + kv.val + "\n";
        output_data.insert(output_data.end(), line.begin(), line.end());
    }

    auto res_lookup_intermediate = chfs_client->lookup(1, intermediate_filename);
    chfs::inode_id_t intermediate_inode_id;
    if (res_lookup_intermediate.is_err()) {
        auto res_mknode = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, intermediate_filename);
        intermediate_inode_id = res_mknode.unwrap();
    } else {
        intermediate_inode_id = res_lookup_intermediate.unwrap();
    }

    auto res_write = chfs_client->write_file(intermediate_inode_id, 0, output_data);
    if (res_write.is_err()) {
        throw std::runtime_error("Failed to write intermediate file");
    }
}

void Worker::doReduce(int index, int nfiles, char start_char, char end_char)
{

    std::unordered_map<std::string, std::vector<std::string>> kvs;
    for (int i = 0; i < nfiles; ++i) {
        std::string intermediate_filename = "mr-" + std::to_string(i);
        auto res_lookup = chfs_client->lookup(1, intermediate_filename);
        if (res_lookup.is_err()) {
            continue;
        }
        auto inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(inode_id, 0, length);
        auto char_vec = res_read.unwrap();
        std::string content(char_vec.begin(), char_vec.end());
        std::istringstream stream(content);
        std::string key, value;
        while (stream >> key >> value) {
            char first_char = std::tolower(key[0]);
            if (first_char >= std::tolower(start_char) && first_char <= std::tolower(end_char)) {
                kvs[key].push_back(value);
            }
        }
    }

    std::vector<chfs::u8> output_data;
    for (const auto &kv : kvs) {
        std::string reduced_value = Reduce(kv.first, kv.second);
        std::string line = kv.first + " " + reduced_value + "\n";
        output_data.insert(output_data.end(), line.begin(), line.end());
    }

    std::string temp_filename = "mr-out-" + std::to_string(index);
    auto res_lookup = chfs_client->lookup(1, temp_filename);
    chfs::inode_id_t output_inode_id;
    if (res_lookup.is_err()) {
        auto res_mknode = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, temp_filename);
        output_inode_id = res_mknode.unwrap();
    } else {
        output_inode_id = res_lookup.unwrap();
    }

    auto res_write = chfs_client->write_file(output_inode_id, 0, output_data);
    if (res_write.is_err()) {
        std::cout << "Failed to write intermediate file" << std::endl;
        throw std::runtime_error("Failed to write intermediate file");
    }
}

void Worker::doMerge()
{
    std::vector<chfs::u8> output_data;
    for (int i = 0; i < nReduce; ++i) {
        std::string temp_filename = "mr-out-" + std::to_string(i);
        auto res_lookup = chfs_client->lookup(1, temp_filename);
        if (res_lookup.is_err()) {
            continue;
        }
        auto inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(inode_id, 0, length);
        auto char_vec = res_read.unwrap();
        output_data.insert(output_data.end(), char_vec.begin(), char_vec.end());
    }

    auto res_lookup = chfs_client->lookup(1, outPutFile);
    chfs::inode_id_t output_inode_id;
    if (res_lookup.is_err()) {
        auto res_mknode = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
        output_inode_id = res_mknode.unwrap();
    } else {
        output_inode_id = res_lookup.unwrap();
    }

    auto res_write = chfs_client->write_file(output_inode_id, 0, output_data);
    if (res_write.is_err()) {
        std::cout << "Failed to write final output file" << std::endl;
        throw std::runtime_error("Failed to write final output file");
    }
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
    // Lab4: Your code goes here.
    mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
}

void Worker::stop()
{
    shouldStop = true;
    work_thread->join();
}

void Worker::doWork()
{
    while (!shouldStop) {
        // Lab4: Your code goes here.
        auto res = mr_client->call(ASK_TASK, 0);
        if (res.is_err()) {
            continue;
        }
        auto task_info = res.unwrap()->as<RpcTaskInfo>();
        if (task_info.type == MAP) {
            doMap(task_info.index, task_info.filename);
            doSubmit(MAP, task_info.index);
        } else if (task_info.type == REDUCE) {
            if (task_info.index == -1) {
                doMerge();
                doSubmit(REDUCE, -1);
                return;
            }
            doReduce(task_info.index, task_info.file_num, task_info.start_char, task_info.end_char);
            doSubmit(REDUCE, task_info.index);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}
} // namespace mapReduce