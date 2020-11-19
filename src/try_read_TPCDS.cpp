#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

// #include "arrow/buffer.h"
// #include "arrow/builder.h"
#include <arrow/array.h>
#include "arrow/result.h"
// #include "arrow/status.h"
#include <arrow/filesystem/filesystem.h>
#include <parquet/arrow/reader.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>

// #include <arrow/io/interfaces.h>
// #include <arrow/memory_pool.h>
// #include <arrow/pretty_print.h>
// #include <arrow/status.h>
// #include <arrow/testing/gtest_util.h>
// #include <gtest/gtest.h>

// #include "arrow/testing/gtest_compat.h"
// #include "arrow/testing/util.h"
// #include "arrow/util/bit_util.h"
// #include "arrow/util/macros.h"
// #include "arrow/util/visibility.h"

#include "utils.h"

#include "ska_sort/ska_sort.hpp"
#include "kxsort/kxsort.h"

const std::string TPCDS_DATA_URI = "file:///home/shelton/data/tpcds_websales_sort_big.parquet";

int main()
{
    std::string file_name;
    auto fs = arrow::fs::FileSystemFromUri(TPCDS_DATA_URI, &file_name).ValueOrDie();
    auto file = fs->OpenInputFile(file_name).ValueOrDie();

    auto pool = arrow::default_memory_pool();
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    assert(parquet::arrow::OpenFile(file, pool, &parquet_reader).ok());

    std::shared_ptr<arrow::Schema> parquet_schmea;
    assert(parquet_reader->GetSchema(&parquet_schmea).ok());
    print(parquet_schmea, "parquet.schema");
    print(parquet_reader->num_row_groups(), "parquet.num_row_groups");

    std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;
    assert(parquet_reader->GetRecordBatchReader({0}, {0, 1, 2}, &record_batch_reader).ok());
    // print(record_batch_reader->schema(), "record_batch_reader");


    // arrow::RecordBatchVector record_batches;
    // record_batch_reader->ReadAll(&record_batches);
    // print(record_batches.size()); // 358
    // print(record_batch->num_columns(), "num_columns");
    // print(record_batch->num_rows(), "num_rows");
    // for each record_batch, cols=3, rows=65536.
    // total 65536*358+22630= 23,484,518 rows

    std::shared_ptr<arrow::RecordBatch> record_batch;
    // size_t num_batches = 0;
    // do {
    //     assert(record_batch_reader->ReadNext(&record_batch).ok());
    //     num_batches += 1;
    // } while (record_batch);
    // print(num_batches, "batches.num");

    assert(record_batch_reader->ReadNext(&record_batch).ok());
    print(record_batch->schema());
    print(record_batch->num_columns());
    print(record_batch_reader->schema()->num_fields());
    auto cols = record_batch->columns();
    auto arr = cols[0];
    print(arr->length());

    // print(record_batch->schema(), "record_batch.schema");
    // arrow::ArrayVector arrays = record_batch->columns();
    // auto array0 = std::dynamic_pointer_cast<arrow::Int32Array>(arrays[0]);
    // std::cout << arrTypedArr0->GetView(0) << std::endl;


    return 0;
}

// auto file_result = fs->OpenInputFile(file_name);
// assert(file_result.ok());
// auto file = file_result.ValueOrDie();