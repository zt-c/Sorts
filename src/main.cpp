#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <arrow/type_traits.h>
#include <parquet/arrow/reader.h>

#include <benchmark/benchmark.h>

#include "utils.h"

#include "kxsort/kxsort.h"
#include "ska_sort/ska_sort.hpp"

const std::string TPCDS_DATA_URI =
    "file:///home/shelton/data/tpcds_websales_sort_big.parquet";

class Sorter {
private:
  const std::string DATA_URI = TPCDS_DATA_URI;

  arrow::MemoryPool *pool;
  std::shared_ptr<arrow::fs::FileSystem> fs;
  std::shared_ptr<arrow::io::RandomAccessFile> file;
  std::string file_name;

  struct ItemIndex {
    size_t array_id;
    size_t id;
    ItemIndex(size_t arrayId, size_t id) : array_id(arrayId), id(id) {}
    std::string ToString() {
      return "{array_id = " + std::to_string(array_id) +
             ", id = " + std::to_string(id) + "}";
    }
  };

public:
  static bool has_shown_info;

  std::vector<std::shared_ptr<arrow::Int32Array>> arrayVector;
  std::vector<ItemIndex> orderings;

  inline int32_t valueAt(ItemIndex item_index) {
    return arrayVector[item_index.array_id]->GetView(item_index.id);
  }

  inline int32_t valueAt(size_t id) { return valueAt(orderings[id]); }

  Sorter() {
    fs = arrow::fs::FileSystemFromUri(DATA_URI, &file_name).ValueOrDie();
    file = fs->OpenInputFile(file_name).ValueOrDie();
    pool = arrow::default_memory_pool();
  }

  void prepareData(bool withRandomization = true) {
    std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
    std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;
    assert(parquet::arrow::OpenFile(file, pool, &parquet_reader).ok());
    assert(parquet_reader
               ->GetRecordBatchReader({0}, {0, 1, 2}, &record_batch_reader)
               .ok());

    std::shared_ptr<arrow::RecordBatch> record_batch;
    size_t array_id = 0;
    // parquet_reader->RowGroup.

    assert(record_batch_reader->ReadNext(&record_batch).ok());

    while (record_batch) {
      auto columns = record_batch->columns();
      std::shared_ptr<arrow::Int32Array> array =
          std::dynamic_pointer_cast<arrow::Int32Array>(columns[0]);
      arrayVector.push_back(array);

      for (size_t i = 0; i != array->length(); ++i) {
        orderings.push_back(ItemIndex(array_id, i));
        // orderings.reserve
        // orderings.emplace_back(array_id,i)
        // uses 2 seconds. TODO: how to optimize?
      }

      array_id += 1;
      assert(record_batch_reader->ReadNext(&record_batch).ok());
    }

    if (withRandomization) {
      std::random_device rd;
      std::mt19937 g(rd());
      std::shuffle(orderings.begin(), orderings.end(), g);
    }
    if (!has_shown_info) {
      print("[ " + std::to_string(orderings.size()) + " items to be sorted. ]");
      has_shown_info = true;
    }
  }

  void stdSort() {
    std::sort(orderings.begin(), orderings.end(),
              [this](ItemIndex lhs, ItemIndex rhs) {
                return valueAt(lhs) > valueAt(rhs);
              });
  }

  void stdStableSort() {
    std::stable_sort(orderings.begin(), orderings.end(),
                     [this](ItemIndex lhs, ItemIndex rhs) {
                       return valueAt(lhs) > valueAt(rhs);
                     });
  }

  void skaSort() {
    ska_sort(orderings.begin(), orderings.end(),
             [this](ItemIndex item_index) { return -valueAt(item_index); });
  }
};

bool Sorter::has_shown_info = false;

void BM_PrepareData(benchmark::State &state) {
  Sorter sorter;
  for (auto _ : state) {
    sorter.prepareData(false);
  }
}
BENCHMARK(BM_PrepareData)->Unit(benchmark::kMillisecond);

void BM_PrepareDataWithRandomization(benchmark::State &state) {
  Sorter sorter;
  for (auto _ : state) {
    sorter.prepareData(true);
  }
}
BENCHMARK(BM_PrepareDataWithRandomization)->Unit(benchmark::kMillisecond);

void BM_StdSort(benchmark::State &state) {
  Sorter sorter;
  sorter.prepareData(false);
  for (auto _ : state) {
    sorter.stdSort();
  }
}
BENCHMARK(BM_StdSort)->Unit(benchmark::kMillisecond);

void BM_StdSortRandomized(benchmark::State &state) {
  Sorter sorter;
  sorter.prepareData(true);
  for (auto _ : state) {
    sorter.stdSort();
  }
}
BENCHMARK(BM_StdSortRandomized)->Unit(benchmark::kMillisecond);

void BM_StdStableSort(benchmark::State &state) {
  Sorter sorter;
  sorter.prepareData(false);
  for (auto _ : state) {
    sorter.stdStableSort();
  }
}
BENCHMARK(BM_StdStableSort)->Unit(benchmark::kMillisecond);

void BM_StdStableSortRandomized(benchmark::State &state) {
  Sorter sorter;
  sorter.prepareData(true);
  for (auto _ : state) {
    sorter.stdStableSort();
  }
}
BENCHMARK(BM_StdStableSortRandomized)->Unit(benchmark::kMillisecond);

void BM_SkaSort(benchmark::State &state) {
  Sorter sorter;
  sorter.prepareData(false);
  for (auto _ : state) {
    sorter.skaSort();
  }
}
BENCHMARK(BM_SkaSort)->Unit(benchmark::kMillisecond);

void BM_SkaSortRandomized(benchmark::State &state) {
  Sorter sorter;
  sorter.prepareData(true);
  for (auto _ : state) {
    sorter.skaSort();
  }
}
BENCHMARK(BM_SkaSortRandomized)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();

// int main(int argc, char const *argv[])
// {
//     Sorter sorter;
//     sorter.prepareData();
//     sorter.skaSort();
//     print(sorter.orderings.size());
//     print(sorter.orderings[0].ToString());
//     print(sorter.valueAt(0));
//     print(sorter.orderings[1].ToString());
//     print(sorter.valueAt(1));
//     print(sorter.orderings[100].ToString());
//     print(sorter.valueAt(100));
//     print(sorter.orderings[200].ToString());
//     print(sorter.valueAt(200));
//     print(sorter.orderings[300].ToString());
//     print(sorter.valueAt(300));
//     print(sorter.orderings[400].ToString());
//     print(sorter.valueAt(400));
//     print(sorter.orderings[23418980].ToString());
//     print(sorter.valueAt(23418980));
//     print(sorter.orderings[23418981].ToString());
//     print(sorter.valueAt(23418981));
//     return 0;
// }
