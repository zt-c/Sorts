add_executable(main main.cpp)
target_link_libraries(main benchmark::benchmark)
target_link_libraries(main arrow_shared)
target_link_libraries(main parquet)

add_executable(try_sorts try_sorts.cpp)
target_link_libraries(try_sorts benchmark::benchmark)
target_link_libraries(try_sorts arrow_shared)

add_executable(try_a try_a.cpp)
add_executable(try_b try_b.cpp)
target_link_libraries(try_b benchmark::benchmark)
target_link_libraries(try_b arrow_shared)

add_executable(try_c try_c.cpp)

add_executable(try_benchmark try_benchmark.cpp)
target_link_libraries(try_benchmark benchmark::benchmark)
target_link_libraries(try_benchmark arrow_shared)

add_executable(try_read_TPCDS try_read_TPCDS.cpp)
target_link_libraries(try_read_TPCDS benchmark::benchmark)
target_link_libraries(try_read_TPCDS arrow_shared)
target_link_libraries(try_read_TPCDS parquet)