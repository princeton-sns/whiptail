add_subdirectory(common)
add_subdirectory(strongstore)
add_subdirectory(nccstore)
add_subdirectory(benchmark/async)

find_library(GPERFTOOLS_PROFILER profiler)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")


add_executable(server ${SourceFiles})
target_include_directories(server PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(server StoreStrong StoreNCC StoreCommon StoreCommonBackend StoreCommonFrontend ReplicationVR ReplicationIR ReplicationCommon Common ${GPERFTOOLS_PROFILER})