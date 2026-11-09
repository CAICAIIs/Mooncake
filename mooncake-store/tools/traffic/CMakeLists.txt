find_package(Threads REQUIRED)

add_executable(traffic_replay traffic_replay.cpp)
target_compile_features(traffic_replay PRIVATE cxx_std_20)
target_include_directories(
  traffic_replay PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../include
                         ${CMAKE_CURRENT_SOURCE_DIR}/../../src/traffic)
target_link_libraries(traffic_replay PRIVATE Threads::Threads
                                             JsonCpp::JsonCpp)
