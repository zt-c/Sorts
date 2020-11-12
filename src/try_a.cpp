#include <iostream>
#include "kxsort/kxsort.h"
//using namespace std;

template <typename T>
struct RadixTraitsArrowArray
{
    static const int nBytes = sizeof(T);
    int kth_byte(const T &x, int k)
    {
        return (x >> (kx::kRadixBits * k)) & kx::kRadixMask;
    }
    bool compare(const T &x, const T &y) { return x < y; }
};

int main(int argc, char const *argv[])
{
    std::string s;
    std::cout << (s=="") << std::endl;
    return 0;
}
