cmake_minimum_required(VERSION 3.16)
project(pando VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_OSX_DEPLOYMENT_TARGET "14.0" CACHE STRING "Minimum macOS version")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Optional: HTTP server (cpp-httplib, header-only)
include(FetchContent)
FetchContent_Declare(httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.14.3
)
FetchContent_MakeAvailable(httplib)

# Optional: RE2 regex library (much faster than std::regex, thread-safe matching)
# Enable with: cmake -DPANDO_USE_RE2=ON ..
# Requires: libre2-dev (apt) or re2 (brew)
option(PANDO_USE_RE2 "Use Google RE2 instead of std::regex" OFF)

if(PANDO_USE_RE2)
    find_package(re2 QUIET)
    if(NOT re2_FOUND)
        # Fallback: try pkg-config (Linux with libre2-dev)
        find_package(PkgConfig QUIET)
        if(PKG_CONFIG_FOUND)
            pkg_check_modules(RE2 REQUIRED re2)
        else()
            message(FATAL_ERROR "RE2 not found. Install with: brew install re2 (macOS) or apt install libre2-dev (Linux)")
        endif()
    endif()
endif()

add_library(pando_core STATIC
    src/core/mmap_file.cpp
    src/index/lexicon.cpp
    src/index/positional_attr.cpp
    src/index/structural_attr.cpp
    src/index/dependency_index.cpp
    src/corpus/corpus.cpp
    src/corpus/builder.cpp
    src/corpus/streaming_builder.cpp
    src/query/lexer.cpp
    src/query/parser.cpp
    src/query/executor.cpp
    src/api/index_api.cpp
)
target_include_directories(pando_core PUBLIC src)

if(PANDO_USE_RE2)
    target_compile_definitions(pando_core PUBLIC PANDO_USE_RE2)
    if(re2_FOUND)
        # Modern CMake target from find_package(re2)
        target_link_libraries(pando_core PUBLIC re2::re2)
    else()
        # pkg-config fallback
        target_include_directories(pando_core PUBLIC ${RE2_INCLUDE_DIRS})
        target_link_libraries(pando_core PUBLIC ${RE2_LIBRARIES})
    endif()
endif()

add_executable(pando-index src/cli/index_main.cpp)
target_link_libraries(pando-index PRIVATE pando_core)

add_executable(pando src/cli/query_main.cpp)
target_link_libraries(pando PRIVATE pando_core)

add_executable(pando-check src/cli/check_main.cpp)
target_link_libraries(pando-check PRIVATE pando_core)

add_executable(cwb-import src/cli/cwb_import_main.cpp)
target_link_libraries(cwb-import PRIVATE pando_core)

# API layer: query execution + JSON (used by CLI and server)
add_library(pando_api STATIC src/api/query_json.cpp)
target_include_directories(pando_api PUBLIC src)
target_link_libraries(pando_api PUBLIC pando_core)

# HTTP API server (optional: requires FetchContent to get cpp-httplib)
add_executable(pando-server src/cli/server_main.cpp)
target_include_directories(pando-server PRIVATE ${httplib_SOURCE_DIR})
target_link_libraries(pando-server PRIVATE pando_api)
