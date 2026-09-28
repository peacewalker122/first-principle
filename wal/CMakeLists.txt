cmake_minimum_required(VERSION 3.20)
project(first_principles_wal LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(wal src/wal.cpp)
target_include_directories(wal PUBLIC include)
target_compile_definitions(wal PRIVATE _POSIX_C_SOURCE=200809L)

include(CTest)

if(BUILD_TESTING)
    find_package(Catch2 3 QUIET)
    if(NOT Catch2_FOUND)
        include(FetchContent)
        FetchContent_Declare(
            Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG v3.7.1
            GIT_SHALLOW TRUE
        )
        FetchContent_MakeAvailable(Catch2)
    endif()

    add_executable(wal_tests tests/wal_smoke.cpp)
    target_link_libraries(wal_tests PRIVATE wal Catch2::Catch2WithMain)
    target_compile_definitions(wal_tests PRIVATE _POSIX_C_SOURCE=200809L CATCH_CONFIG_NO_COUNTER)

    include(Catch)
    catch_discover_tests(wal_tests)
endif()
