cmake_minimum_required(VERSION 3.14)
project(raftkv VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_library(raftkv src/node.cpp src/cluster.cpp src/scenarios.cpp)
target_include_directories(raftkv PUBLIC include)
if(MSVC)
  target_compile_options(raftkv PRIVATE /W4)
else()
  target_compile_options(raftkv PRIVATE -Wall -Wextra)
endif()

add_executable(raftkv_demo src/main.cpp)
target_link_libraries(raftkv_demo PRIVATE raftkv)

enable_testing()
add_executable(raftkv_tests tests/test_raftkv.cpp)
target_link_libraries(raftkv_tests PRIVATE raftkv)
add_test(NAME raftkv_tests COMMAND raftkv_tests)
