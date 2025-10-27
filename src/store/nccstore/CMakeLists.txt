protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${CMAKE_SOURCE_DIR}/store/common/common-proto.proto ncc-proto.proto)

file(GLOB SourceFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(StoreNCC ${SourceFiles} ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(StoreNCC PUBLIC ${CMAKE_SOURCE_DIR} ${CMAKE_BINARY_DIR})
target_link_libraries(StoreNCC StoreCommon StoreCommonBackend StoreCommonFrontend ReplicationVR ReplicationIR ReplicationCommon ${PROTOBUF_LIBRARY})

