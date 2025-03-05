# src/workload/random/CMakeLists.txt

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.*")

add_library(workload_random ${SourceFiles})

