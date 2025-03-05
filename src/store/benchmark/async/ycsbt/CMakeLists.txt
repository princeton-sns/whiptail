file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_subdirectory(ycsbt)
add_subdirectory(ycsb)
add_subdirectory(random)

add_library(BenchmarkYcsbt ${SourceFiles})
target_include_directories(BenchmarkYcsbt PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(BenchmarkYcsbt BenchmarkCommon StoreCommonFrontend workload_random workload_ycsb workload_ycsbt)