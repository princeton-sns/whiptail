add_subdirectory(common)
add_subdirectory(retwis)
add_subdirectory(ycsbt)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")


find_library(GPERFTOOLS_PROFILER profiler)
find_library(GPERFTOOLS_TCMALLOC tcmalloc)
add_executable(benchmark ${SourceFiles})
target_include_directories(benchmark PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})

message(STATUS "Found gperftools profiler: ${GPERFTOOLS_PROFILER}")

target_link_libraries(benchmark BenchmarkCommon BenchmarkRetwis BenchmarkYcsbt StoreStrong StoreNCC Common ${GPERFTOOLS_PROFILER})