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

class Sorter
{
private:
    const std::string DATA_URI = TPCDS_DATA_URI;

    arrow::MemoryPool *pool;
    std::shared_ptr<arrow::fs::FileSystem> fs;
    std::shared_ptr<arrow::io::RandomAccessFile> file;
    std::string file_name;

    struct ItemIndex
    {
        size_t array_id;
        size_t id;
        ItemIndex(size_t arrayId, size_t id) : array_id(arrayId), id(id) {}
        std::string ToString() { return "{array_id = " + std::to_string(array_id) + ", id = " + std::to_string(id) + "}"; }
    };

public:
    // arrow::ArrayVector arrayVector;
    std::vector<std::shared_ptr<arrow::Int32Array>> arrayVector;
    std::vector<ItemIndex> orderings;

    Sorter()
    {
        fs = arrow::fs::FileSystemFromUri(DATA_URI, &file_name).ValueOrDie();
        file = fs->OpenInputFile(file_name).ValueOrDie();
        pool = arrow::default_memory_pool();
    }

    void prepareData()
    {
        std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
        std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;
        assert(parquet::arrow::OpenFile(file, pool, &parquet_reader).ok());
        assert(parquet_reader->GetRecordBatchReader({0}, {0, 1, 2}, &record_batch_reader).ok());

        std::shared_ptr<arrow::RecordBatch> record_batch;
        size_t array_id = 0;

        assert(record_batch_reader->ReadNext(&record_batch).ok());

        while (record_batch)
        {
            auto columns = record_batch->columns();
            std::shared_ptr<arrow::Int32Array> array = std::dynamic_pointer_cast<arrow::Int32Array>(columns[0]);
            arrayVector.push_back(array);

            for (size_t i = 0; i != array->length(); ++i)
            {
                orderings.push_back(ItemIndex(array_id, i));
            }

            array_id += 1;
            assert(record_batch_reader->ReadNext(&record_batch).ok());
        }
    }

    void stdSort()
    {
        std::sort(orderings.begin(), orderings.end(),
                  [this](ItemIndex lhs, ItemIndex rhs) {
                      return arrayVector[lhs.array_id]->GetView(lhs.id) >
                             arrayVector[rhs.array_id]->GetView(rhs.id);
                  });
    }

    void stdStableSort()
    {
        std::stable_sort(orderings.begin(), orderings.end(),
                  [this](ItemIndex lhs, ItemIndex rhs) {
                      return arrayVector[lhs.array_id]->GetView(lhs.id) >
                             arrayVector[rhs.array_id]->GetView(rhs.id);
                  });
    }

    void skaSort()
    {
        ska_sort(orderings.begin(), orderings.end(),
                 [this](ItemIndex item_index) {
                     return -arrayVector[item_index.array_id]->GetView(item_index.id);
                 });
    }
};

void BMStdSort(benchmark::State &state)
{
    Sorter sorter;
    for (auto _ : state)
    {
        sorter.prepareData();
        sorter.stdSort();
    }
}
BENCHMARK(BMStdSort);

void BMStdStableSort(benchmark::State &state)
{
    Sorter sorter;
    for (auto _ : state)
    {
        sorter.prepareData();
        sorter.stdStableSort();
    }
}
BENCHMARK(BMStdStableSort);

void BMSkaSort(benchmark::State &state)
{
    Sorter sorter;
    for (auto _ : state)
    {
        sorter.prepareData();
        sorter.skaSort();
    }
}
BENCHMARK(BMSkaSort);

BENCHMARK_MAIN();

/**
Running /home/shelton/workspace/sorts/build/src/main
Run on (88 X 3600 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x44)
  L1 Instruction 32 KiB (x44)
  L2 Unified 256 KiB (x44)
  L3 Unified 56320 KiB (x2)
Load Average: 0.33, 0.37, 0.24
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
***WARNING*** Library was built as DEBUG. Timings may be affected.
----------------------------------------------------------
Benchmark                Time             CPU   Iterations
----------------------------------------------------------
BMStdSort       38186149909 ns   38183498563 ns            1
BMStdStableSort 34021783775 ns   34018477126 ns            1
BMSkaSort       8634802731 ns   8634126462 ns            1
 */

// int main(int argc, char const *argv[])
// {
//     Sorter sorter;
//     sorter.prepareData();
//     print(sorter.orderings.size());
//     print(sorter.orderings[23418980].ToString());
//     print(sorter.orderings[23418981].ToString());
//     return 0;
// }
