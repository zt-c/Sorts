#include <iostream>
#include <numeric>
#include <vector>
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/buffer_builder.h>
#include <arrow/builder.h>
#include <arrow/compute/context.h>
#include <arrow/compute/expression.h>
#include <arrow/compute/logical_type.h>
#include <arrow/type_traits.h>
#include <arrow/visitor_inline.h>


int main(int argc, char const *argv[])
{
    std::vector<int> vi(10);
    std::iota(std::next(vi.begin(), 5), std::next(vi.begin(), 9), 3);

    for (auto i: vi) {
        std::cout << i << " ";
    }
    std::cout << std::endl;

    arrow::Int64Builder builder;
    builder.Resize(8);
    std::vector<bool> validity = {1, 1, 1, 0, 0, 1, 1, 1};
    std::vector<int64_t> values = {1, 2, 3, 4, 5, 6, 7, 8};
    builder.AppendValues(values, validity);

    std::shared_ptr<arrow::Int64Array> array;
    arrow::Status st = builder.Finish(&array);


    // std::cout << "---" << std::endl;

    for (size_t i = 0; i != array->length(); ++i)
    {
        std::cout << array->Value(i) << " ";
    }
    std::cout << std::endl;

    for (size_t i = 0; i != array->length(); ++i)
    {
        std::cout << array->Value(i) << " ";
    }
    std::cout << std::endl;
    return 0;
}