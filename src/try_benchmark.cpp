#include <iostream>
#include <string>
#include <array>
#include <unistd.h>

#include <benchmark/benchmark.h>

void b1(benchmark::State& state)
{
    for (auto _: state) {
        std::string s;
    }
}

BENCHMARK(b1);

void b2(benchmark::State& state)
{
    for (auto _: state) {
        int i;
    }
}

BENCHMARK(b2);

void b3(benchmark::State& state)
{
    std::string x = "aaaaaaaaaaaaaaaaa";
    std::string copy2(x);
    for (auto _: state) {
    std::string copy1(x);
        std::string copy(x);
    }
}

BENCHMARK(b3);

void b4(benchmark::State& state)
{
    for (auto _ : state)
    {
        // std::string copy("1");
    }
}

BENCHMARK(b4);

class C {
    void b5(benchmark::State& state) {
        for (auto _ : state) {
            std::string copy("1");
        }
    }
};


BENCHMARK_MAIN();
