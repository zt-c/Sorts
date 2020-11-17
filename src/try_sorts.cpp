#include <iostream>
#include <vector>
#include <algorithm>
#include <random>
#include <numeric>

#include "utils.h"

#include "ska_sort/ska_sort.hpp"
#include "kxsort/kxsort.h"

template<class T>
struct RadixTraitsSignedDec {
    static const int nBytes = sizeof(T);
    static const T kMSB = T(0x80) << ((sizeof(T) - 1) * 8);
    static const int tttttttttttt = 1;
    int kth_byte (const T &x, int k) {
        return ((x ^ kMSB) >> (kx::kRadixBits * k)) & kx::kRadixMask;
    }
    bool compare(const T &x, const T &y)
    {
        std::cout << "---" <<  x << " " << y << std::endl;
        return x > y;
    }
};


int main()
{
    std::vector<int32_t> data(100000);
    std::iota(data.begin(), data.end(), 0);
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);

    // print(data);

    // ska_sort(data.begin(), data.end(), [](auto &x) { return x; });
    kx::radix_sort(data.begin(), data.end(), RadixTraitsSignedDec<int32_t>());
    // kx::radix_sort(data.begin(), data.end());
    // print(data);

    return 0;
}
