#include <iostream>
#include <numeric>
#include <vector>
#include <memory>
#include "utils.h"

int main(int argc, char const *argv[])
{
    print(sizeof(int8_t));
    print(sizeof(int16_t));
    print(sizeof(int));
    print(sizeof(int32_t));
    print(sizeof(int64_t));
    print(sizeof(uint64_t));
    print(sizeof(__int128_t));
    return 0;
}
