# src/workload/ycsb/CMakeLists.txt

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library(workload_ycsb ${SourceFiles})

