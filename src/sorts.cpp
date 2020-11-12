#include <iostream>
#include <vector>
#include <algorithm>

#include "utils.h"

#include "ska_sort/ska_sort.hpp"
#include "kxsort/kxsort.h"

int main()
{
    std::vector<uint> data1 = {4, 1, 59, 3, 23, 95};
    std::vector<uint> data2 = {4, 1, 59, 3, 23, 95};
    ska_sort(data1.begin(), data1.end(), [](auto &x) { return -x; });

    // kx::radix_sort(data2.begin(), data2.end(), [](auto &x) { return -x; });

    print(data1);
    print(data2);
    return 0;
}
