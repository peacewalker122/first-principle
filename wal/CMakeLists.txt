cmake_minimum_required(VERSION 3.20)
project(first_principles_wal LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(wal src/wal.cpp)
target_include_directories(wal PUBLIC include)
target_compile_definitions(wal PRIVATE _POSIX_C_SOURCE=200809L)

add_executable(wal_smoke tests/wal_smoke.cpp)
target_link_libraries(wal_smoke PRIVATE wal)
target_compile_definitions(wal_smoke PRIVATE _POSIX_C_SOURCE=200809L)

enable_testing()
add_test(NAME wal_smoke COMMAND wal_smoke)
