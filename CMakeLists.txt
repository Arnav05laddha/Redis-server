cmake_minimum_required(VERSION 3.13)
project(codecrafters-redis CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wno-unused-parameter -O2)

# Files are in the project root (not src/)
add_executable(server server.cpp)

target_include_directories(server PRIVATE .)

find_package(Threads REQUIRED)
target_link_libraries(server Threads::Threads)
