#include <iostream>
#include <memory>
#include <numeric>
#include <vector>
#include "utils.h"

struct ValueIndex {
  size_t file_id, column_id, array_id, array_index;
  std::string ToString() {
    char s[100];
    std::sprintf(
        s, "{file_id = %d, column_id = %d, array_id = %d, array_index = %d}",
        file_id, column_id, array_id, array_index);
    std::string ans(s);
    return ans;
  }
};

void f() {
  std::vector<int> vi;
  vi.reserve(10);
  print(vi.size());
  // vi.emplace_back(

  // )
  print(vi);
}

int main(int argc, char const *argv[]) {
  int i = 1;
  std::vector<std::vector<int>> v(10);
  v.resize(15);
  // v.push_back(std::vector<int>());
  print(v.size());
  print(v[0].size());
  return 0;
}
