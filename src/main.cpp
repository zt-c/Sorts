#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/filesystem/filesystem.h>
#include <parquet/arrow/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

#include <benchmark/benchmark.h>

#include "utils.h"

#include "ska_sort/ska_sort.hpp"
#include "kxsort/kxsort.h"

const std::string TPCDS_DATA_URI = "file:///home/shelton/data/tpcds_websales_sort_big.parquet";

class Sorts
{
private:
    const std::string DATA_URI = TPCDS_DATA_URI;

    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::shared_ptr<arrow::io::RandomAccessFile> file;
    std::string file_name;
    // std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    // std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;
    arrow::MemoryPool *pool;

public:
    arrow::ArrayVector arrayVector;

public:
    Sorts()
    {
        fs = arrow::fs::FileSystemFromUri(DATA_URI, &file_name).ValueOrDie();
        file = fs->OpenInputFile(file_name).ValueOrDie();
        pool = arrow::default_memory_pool();
    }


    // std::stable_sort(indices_begin, nulls_begin,
    //                 [&values](uint64_t left, uint64_t right) {
    //                 return values.GetView(left) < values.GetView(right);
    //                 });
    void stdSort()
    {
        // std::shared_ptr<arrow::RecordBatch> record_batch;
        // // auto st = record_batch_reader->ReadNext(&record_batch);

        // // arrow::ArrayVector arrays = record_batch->columns();
        // // auto values = std::dynamic_pointer_cast<arrow::Int32Array>(arrays[0]);

        // // std::iota(indices_begin, indices_end, 0);
        // // std::sort(indices_begin, indices_end,
        // //           [&values](int64_t left, int64_t right) {
        // //               return values->GetView(left) > values->GetView(right);
        // //           });

        // size_t num_batches = 0;
        // do {
        //     assert(record_batch_reader->ReadNext(&record_batch).ok());
        //     num_batches += 1;
        //     if (record_batch == nullptr) { break; }
        //     arrow::ArrayVector arrays = record_batch->columns();
        //     auto values = std::dynamic_pointer_cast<arrow::Int32Array>(arrays[0]);

        //     int64_t buf_size = values->length() * sizeof(int64_t);
        //     std::shared_ptr<arrow::Buffer> indices_buf = arrow::AllocateBuffer(buf_size, pool).ValueOrDie();
        //     int64_t *indices_begin = reinterpret_cast<int64_t *>(indices_buf->mutable_data());
        //     int64_t *indices_end = indices_begin + values->length();

        //     std::iota(indices_begin, indices_end, 0);
        //     std::sort(indices_begin, indices_end,
        //               [&values](int64_t left, int64_t right) {
        //                   return values->GetView(left) > values->GetView(right);
        //               });

        // } while (record_batch);
        // print(num_batches, "batches.num");
    }

    arrow::ArrayVector prepareData()
    {
        std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
        std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;
        assert(parquet::arrow::OpenFile(file, pool, &parquet_reader).ok());
        assert(parquet_reader->GetRecordBatchReader({0}, {0, 1, 2}, &record_batch_reader).ok());

        std::shared_ptr<arrow::RecordBatch> record_batch;
        size_t num_batches = 0;
        arrow::ArrayVector arrayVector;

        assert(record_batch_reader->ReadNext(&record_batch).ok());
        num_batches += 1;

        while (record_batch) {
            auto columns = record_batch->columns();
            std::shared_ptr<arrow::Int32Array> array = std::dynamic_pointer_cast<arrow::Int32Array>(columns[0]);
            arrayVector.push_back(array);

            assert(record_batch_reader->ReadNext(&record_batch).ok());
            num_batches += 1;
        }

        return arrayVector;
    }
};

void b1(benchmark::State &state)
{
    Sorts sorts;
    for (auto _ : state)
    {
        sorts.stdSort();
    }
}

// BENCHMARK(b1);
// BENCHMARK_MAIN();

int main(int argc, char const *argv[])
{
    Sorts sorts;
    auto data = sorts.prepareData();
    // auto data = sorts.arrayVector;
    // print(sizeof(data));
    return 0;
}
