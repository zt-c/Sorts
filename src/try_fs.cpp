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

int main(int argc, char const *argv[]) {
  std::string dataset_dir;
  std::shared_ptr<arrow::fs::FileSystem> fs =
      arrow::fs::FileSystemFromUri(DATA_URI, &dataset_dir).ValueOrDie();

  arrow::fs::FileSelector dataset_dir_selector;
  dataset_dir_selector.base_dir = dataset_dir;
  std::vector<arrow::fs::FileInfo> file_infos =
      fs->GetFileInfo(dataset_dir_selector).ValueOrDie();

  std::vector<std::shared_ptr<arrow::io::RandomAccessFile>> files;
  for (const auto &file_info : file_infos) {
    auto file = fs->OpenInputFile(file_info.path()).ValueOrDie();
    files.push_back(file);
  }
  auto pool = arrow::default_memory_pool();
  std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
  std::unique_ptr<arrow::RecordBatchReader> record_batch_reader;

  // print(files.size());
  // for (const auto &file : files) {
  //   assert(parquet::arrow::OpenFile(file, pool, &parquet_reader).ok());
  //   print(parquet_reader->num_row_groups());
  //   assert(parquet_reader->GetRecordBatchReader({0}, {0}, &record_batch_reader)
  //              .ok()); // {0}, {0..11}
  //   print(record_batch_reader->schema());
  // }

  // std::shared_ptr<arrow::Schema> parquet_schmea;
  // assert(parquet_reader->GetSchema(&parquet_schmea).ok());
  // print(parquet_schmea);

  assert(parquet::arrow::OpenFile(files[0], pool, &parquet_reader).ok());
  std::vector<int> column_indices = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
  assert(parquet_reader->GetRecordBatchReader({0}, column_indices, &record_batch_reader) .ok());
  // print(record_batch_reader->schema());
  std::shared_ptr<arrow::RecordBatch> record_batch;
  assert(record_batch_reader->ReadNext(&record_batch).ok());


  std::vector<std::shared_ptr<arrow::Array>> raw_columns = record_batch->columns();
  std::vector<std::shared_ptr<arrow::Int32Array>> columns;
  for (auto raw_column : raw_columns) {
    columns.push_back(std::dynamic_pointer_cast<arrow::Int32Array>(raw_column));
  }
  // print(columns[0]->GetView(2));

  // std::vector<std::shared_ptr<arrow::Array>> columns = record_batch->columns();
  // print(record_batch_reader->schema());
  // print(columns.size());
  // print(columns[0]->null_count());
  // print(columns[0]->GetView(0));

  // print(std::dynamic_pointer_cast<arrow::Int32Array>(raw_columns[0])->GetView(0));
  // print(std::dynamic_pointer_cast<arrow::NumericArray<(rawcolomns[0]->type())>(columns[0])->GetView(0));
  // print(columns[1]->null_count());
  // print(columns[2]->null_count());
  // print(columns[0]->length());

  return 0;
}