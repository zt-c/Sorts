
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "utils.h"

const std::string DATA_URI =
    "file:///home/shelton/data/tpcds_websales_partitioned";

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
    // size_t array_id, array_index, column_id;
    size_t array_id, array_index;

    std::string ToString() {
      char s[100];
      std::sprintf(s, "{array_id = %ld, array_index = %ld}", array_id,
                   array_index);
      return std::string(s);
    }
  };

 public:
  static bool has_shown_info_;

  std::vector<std::vector<std::shared_ptr<arrow::Int32Array>>> data_;
  std::vector<ValueIndex> orderings_;

  inline int32_t ValueAt(size_t column_id, const ValueIndex &value_index) {
    return data_[column_id][value_index.array_id]->GetView(
        value_index.array_index);
  }

  inline int32_t ValueAt(size_t column_id, size_t idx) {
    return ValueAt(column_id, orderings_[idx]);
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
      std::sprintf(s, "[ ... data ready: %ld entries ... ]", orderings_.size());
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
    orderings_.reserve(data_[0].size() * data_[0][0]->length());

    for (size_t array_id = 0; array_id != data_[0].size(); ++array_id) {
      for (size_t array_index = 0; array_index != data_[0][array_id]->length();
           ++array_index) {
        orderings_.emplace_back(ValueIndex({array_id, array_index}));
      }
    }
  }
};

int main(int argc, char const *argv[]) {
  Sorter sorter;
  sorter.InitData();
  sorter.InitOrderings();
  print(sorter.data_[0].size());
  print(sorter.data_[0][0]->length());
  print(sorter.orderings_.size());
  return 0;
}
// sorter.Init();
// print(sorter.ValueAt(0, 0));
// print(sorter.ValueAt(0, 1));
// print(sorter.ValueAt(0, 2));
// print(sorter.orderings_.size());
// print(sorter.orderings_[0].size());
// print(sorter.ValueAt(11,14476450));
// print(sorter.ValueAt(11,14476451));
// print(sorter.ValueAt(11,14476452));
