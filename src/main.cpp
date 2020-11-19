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

// const std::string TPCDS_DATA_URI =
// "file:///home/shelton/data/tpcds_websales_sort_big.parquet";
const std::string TPCDS_DATA_URI =
    "file:///home/shelton/data/tpcds_websales_partitioned";

class Sorter {
 private:
  const std::string DATASET_URI = TPCDS_DATA_URI;

  arrow::MemoryPool *pool_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::string dataset_dir_;
  std::vector<arrow::fs::FileInfo> file_infos_;
  std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files_;

  struct ValueIndex {
    size_t array_id, array_index;

    std::string ToString() {
      char s[40];
      std::sprintf(s, "{array_id = %ld, array_index = %ld}", array_id,
                   array_index);
      return std::string(s);
    }
  };

 public:
  static bool has_shown_info_;

  std::vector<std::vector<std::shared_ptr<arrow::Int32Array>>> data_;
  std::vector<std::vector<ValueIndex>> orderings_;

  inline int32_t ValueAt(size_t column_id, const ValueIndex &idx) {
    return data_[column_id][idx.array_id]->GetView(idx.array_index);
  }

  inline int32_t ValueAt(size_t column_id, size_t idx) {
    return ValueAt(column_id, orderings_[column_id][idx]);
  }

  Sorter() {
    pool_ = arrow::default_memory_pool();
    fs_ = arrow::fs::FileSystemFromUri(DATASET_URI, &dataset_dir_).ValueOrDie();

    arrow::fs::FileSelector dataset_dir_selector;
    dataset_dir_selector.base_dir = dataset_dir_;

    file_infos_ = fs_->GetFileInfo(dataset_dir_selector).ValueOrDie();

    for (const auto &file_info : file_infos_) {
      auto file = fs_->OpenInputFile(file_info.path()).ValueOrDie();
      files_.push_back(file);
    }
  }

  void Init() {
    InitData();
    InitOrderings();
    if (!has_shown_info_) {
      char s[40];
      std::sprintf(s,"[ ... sorting %ld values ... ]", orderings_[0].size());
      // std::cout << std::string(s) << std::endl;
      print(std::string(s));
      has_shown_info_ = true;
    }
  }

  void InitData() {
    data_.resize(12);
    for (auto file : files_) {
      std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
      assert(parquet::arrow::OpenFile(file, pool_, &parquet_reader).ok());

      std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;
      std::vector<int> column_indices = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
      assert(
          parquet_reader
              ->GetRecordBatchReader({0}, column_indices, &record_batch_reader)
              .ok());

      std::shared_ptr<arrow::RecordBatch> record_batch;
      for (;;) {
        assert(record_batch_reader->ReadNext(&record_batch).ok());
        if (!record_batch) break;
        std::vector<std::shared_ptr<arrow::Array>> raw_columns =
            record_batch->columns();
        for (size_t column_id = 0; column_id != raw_columns.size();
             ++column_id) {
          data_[column_id].emplace_back(
              std::dynamic_pointer_cast<arrow::Int32Array>(
                  raw_columns[column_id]));
        }
      }
    }
  }
  void InitOrderings() {
    // 12 columns
    orderings_.resize(data_.size());

    for (size_t column_id = 0; column_id != data_.size(); ++column_id) {
      orderings_[column_id].reserve(data_[column_id].size() *
                                    data_[column_id][0]->length());

      for (size_t array_id = 0; array_id != data_[column_id].size();
           ++array_id) {
        for (size_t array_index = 0;
             array_index != data_[column_id][array_id]->length();
             ++array_index) {
          orderings_[column_id].emplace_back(
              ValueIndex({array_id, array_index}));
        }
      }
    }
  }

  void stdSort() {
    std::sort(orderings_[0].begin(), orderings_[0].end(),
              [this](ValueIndex lhs, ValueIndex rhs) {
                return ValueAt(0, lhs) > ValueAt(0, rhs);
              });
  }

  void stdStableSort() {
    std::stable_sort(orderings_[0].begin(), orderings_[0].end(),
                     [this](ValueIndex lhs, ValueIndex rhs) {
                       return ValueAt(0, lhs) > ValueAt(0, rhs);
                     });
  }

  void skaSort() {
    ska_sort(
        orderings_[0].begin(), orderings_[0].end(),
        [this](ValueIndex value_index) { return -ValueAt(0, value_index); });
  }
};

bool Sorter::has_shown_info_ = false;

void BM_InitData(benchmark::State &state) {
  Sorter sorter;
  for (auto _ : state) {
    sorter.InitData();
  }
}
BENCHMARK(BM_InitData)->Unit(benchmark::kMillisecond);

void BM_InitOrderings(benchmark::State &state) {
  Sorter sorter;
  for (auto _ : state) {
    sorter.InitData();
    sorter.InitOrderings();
  }
}
BENCHMARK(BM_InitOrderings)->Unit(benchmark::kMillisecond);

void BM_StdSort(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdSort();
  }
}
BENCHMARK(BM_StdSort)->Unit(benchmark::kMillisecond);

void BM_StdStableSort(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.stdStableSort();
  }
}
BENCHMARK(BM_StdStableSort)->Unit(benchmark::kMillisecond);

void BM_SkaSort(benchmark::State &state) {
  Sorter sorter;
  sorter.Init();
  for (auto _ : state) {
    sorter.skaSort();
  }
}
BENCHMARK(BM_SkaSort)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();

// int main(int argc, char const *argv[])
// {
//     Sorter sorter;
//     sorter.prepareData();
//     sorter.skaSort();
//     print(sorter.orderings_.size());
//     print(sorter.orderings_[0].ToString());
//     print(sorter.valueAt(0));
//     print(sorter.orderings_[1].ToString());
//     print(sorter.valueAt(1));
//     print(sorter.orderings_[100].ToString());
//     print(sorter.valueAt(100));
//     print(sorter.orderings_[200].ToString());
//     print(sorter.valueAt(200));
//     print(sorter.orderings_[300].ToString());
//     print(sorter.valueAt(300));
//     print(sorter.orderings_[400].ToString());
//     print(sorter.valueAt(400));
//     print(sorter.orderings_[23418980].ToString());
//     print(sorter.valueAt(23418980));
//     print(sorter.orderings_[23418981].ToString());
//     print(sorter.valueAt(23418981));
//     return 0;
// }
