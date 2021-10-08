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

using namespace ROCKSDB_NAMESPACE;

struct io_uring *ioring = nullptr;
bool debug_mode = false;
int loop = 1;
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
//    std::string key, value;
    std::string res;
//    std::cin >> key;
//    std::cin >> value;
    auto result = db->AsyncPut(WriteOptions(), db->DefaultColumnFamily(), "test", "abc");
    co_await result;
    std::cout << "resume result, done" << std::endl;
    Status s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "test", &res);
    assert(s.ok());
    std::cout << res << std::endl;
    co_return s;
}

Status normal_test() {
//    std::string key, value;
    std::string res;
//    std::cin >> key;
//    std::cin >> value;
    auto result = db->Put(WriteOptions(), db->DefaultColumnFamily(), "test", "abc");
    Status s = db->Get(ReadOptions(), db->DefaultColumnFamily(), "test", &res);
    assert(s.ok());
    std::cout << res << std::endl;
    return s;
}

static void reap_iouring_completion(struct io_uring *ring) {
    struct io_uring_cqe *cqe;
    while (true) {
        auto ret = io_uring_wait_cqe(ring, &cqe);
        if (ret == 0 && cqe->res >= 0) {
            struct file_page *rdata = (file_page *) io_uring_cqe_get_data(cqe);
            io_uring_cqe_seen(ring, cqe);

            auto h = std::coroutine_handle<async_result::promise_type>::from_promise(*rdata->promise);
            h.resume();
            return;
        }
    }
}

int main(int argc, char *argv[]) {
    // init ioring and start a thread
    ioring = new io_uring();
    io_uring_queue_init(loop, ioring, 0);
    std::thread t1(reap_iouring_completion, ioring);
    t1.detach();
    // start insert key and read test
    open_database();
    for (int i = 0; i < loop; ++i) {
        if (*argv[1] == 'a') {
            auto result = async_test();
        } else {
            auto result = normal_test();
        }
//        while(!result.is_result_set()) {
//            std::this_thread::sleep_for(std::chrono::seconds(1));
//        }
    }
    std::this_thread::sleep_for(std::chrono::seconds(30));
    close_database();

}
