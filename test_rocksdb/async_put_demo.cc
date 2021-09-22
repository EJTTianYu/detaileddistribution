#include <time.h>
#include <string>
#include <memory>
#include <iostream>
#include <thread>         // std::this_thread::sleep_for
#include <chrono>         // std::chrono::seconds
#include <exception>
#include "liburing.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/async_result.h"
#include "executor.h"
#include "placement_strategy.h"

int loop = 5;
std::string kDBPath = "/tmp/rocksdb_simple_example";
DB *db;

//
// Created by 二阶堂天宇 on 2021/9/22.
//
void open_database() {
    Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    options.write_buffer_size = 64 << 20; //1mb
    options.max_write_buffer_number = 1;
    options.min_write_buffer_number_to_merge = 2;
    options.use_direct_reads = true;
    options.use_direct_io_for_flush_and_compaction = false;
    BlockBasedTableOptions block_based_options;
    block_based_options.no_block_cache = true;
    options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

    // open DB
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());
}

void close_database() {
    db->Close();
}

async_result async_test() {
    std::string key, value;
    std::string res;
    std::cin >> key;
    std::cin >> value;
    auto result = db->AsyncPut(WriteOptions(), db->DefaultColumnFamily(), key, value);
    co_await result;
    s = db->Get(ReadOptions(), db->DefaultColumnFamily(), key, &res);
    assert(s.ok());
    std::cout << res << std::endl;
    co_return s;
}

int main(int argc, char *argv[]) {

    open_database();
    for (int i = 0; i < loop; ++i) {
        auto result = async_test();
        while(!result.is_result_set()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
    close_database();

}
