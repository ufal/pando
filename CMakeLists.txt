cmake_minimum_required(VERSION 3.16)
# Minimum macOS for the binaries (must come before project(); without it the
# toolchain targets the build host's own macOS version, which older Macs can't run).
# Override with -DCMAKE_OSX_DEPLOYMENT_TARGET=…
set(CMAKE_OSX_DEPLOYMENT_TARGET "14.0" CACHE STRING "Minimum macOS version")
project(pando VERSION 0.1.20 LANGUAGES C CXX)

include(CTest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Optional: HTTP server (cpp-httplib, header-only)
include(FetchContent)
if(POLICY CMP0169)
    # FetchContent_Populate() is deprecated on newer CMake; CMP0169=OLD keeps this project quiet.
    cmake_policy(SET CMP0169 OLD)
endif()
FetchContent_Declare(httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.14.3
)
FetchContent_MakeAvailable(httplib)

# Regex engine for attribute values. RE2 (libre2-dev / brew re2): UTF-8 aware
# (`.` = one character), `(?i)`, 3-4x faster lexicon scans; patterns RE2 cannot
# compile (backreferences, lookaround) still run on std::regex (core/regex_engine.h).
#   AUTO (default): RE2 when found, else std::regex;  ON: RE2 required;  OFF: std::regex only.
set(PANDO_USE_RE2 "AUTO" CACHE STRING "Regex engine: AUTO | ON | OFF")
set_property(CACHE PANDO_USE_RE2 PROPERTY STRINGS AUTO ON OFF)

# CWB/CQP dialect: optional static library (GPL-2+ if derived from IMS CWB sources). Can be
# disabled for minimal builds or shipped separately; see dev/CQL-DIALECT-LICENSING.md.
option(PANDO_CWB_DIALECT "Build CWB/CQP lexer, parser, and translate_cwb_program" ON)

# PML-TQ dialect: lexer/parser scaffold + translate_pmltq_program (see dev/PMLTQ-ROADMAP.md).
option(PANDO_PMLTQ_DIALECT "Build PML-TQ lexer, parser stub, and translate_pmltq_program" ON)

set(PANDO_WITH_RE2 OFF)
string(TOUPPER "${PANDO_USE_RE2}" _pando_re2_mode)
if(NOT _pando_re2_mode STREQUAL "OFF" AND NOT _pando_re2_mode STREQUAL "FALSE" AND NOT _pando_re2_mode STREQUAL "0")
    find_package(re2 QUIET)
    if(re2_FOUND)
        set(PANDO_WITH_RE2 ON)
    else()
        # pkg-config (Linux with libre2-dev)
        find_package(PkgConfig QUIET)
        if(PKG_CONFIG_FOUND)
            pkg_check_modules(RE2 QUIET re2)
            if(RE2_FOUND)
                set(PANDO_WITH_RE2 ON)
            endif()
        endif()
    endif()
    if(NOT PANDO_WITH_RE2 AND NOT _pando_re2_mode STREQUAL "AUTO")
        message(FATAL_ERROR "RE2 not found. Install with: brew install re2 (macOS) or apt install libre2-dev (Linux), or -DPANDO_USE_RE2=OFF")
    endif()
endif()
message(STATUS "pando regex engine: ${PANDO_WITH_RE2} (RE2; OFF = std::regex)  [PANDO_USE_RE2=${PANDO_USE_RE2}]")

if(PANDO_CWB_DIALECT)
    add_library(pando_cwb_dialect STATIC
        src/query/dialect/cwb/cwb_translate.cpp
        src/query/dialect/cwb/cwb_lexer.cpp
        src/query/dialect/cwb/cwb_parser.cpp
    )
else()
    add_library(pando_cwb_dialect STATIC
        src/query/dialect/cwb/cwb_translate_stub.cpp
    )
endif()
target_include_directories(pando_cwb_dialect PUBLIC src)

# Default path to interim PEG bridge script (dev/pmltq-gold/parse_ast_sql.js); override with PMLTQ_GOLD_SCRIPT at runtime.
# Use CURRENT_SOURCE_DIR so this tree still resolves when Pando is add_subdirectory()'d (e.g. flexicorp_pando).
set(PMLTQ_GOLD_SCRIPT_PATH "${CMAKE_CURRENT_SOURCE_DIR}/dev/pmltq-gold/parse_ast_sql.js")
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/pmltq_gold_paths.cpp.in"
    "${CMAKE_BINARY_DIR}/pmltq_gold_paths.cpp"
    @ONLY)

if(PANDO_PMLTQ_DIALECT)
    add_library(pando_pmltq_dialect STATIC
        "${CMAKE_BINARY_DIR}/pmltq_gold_paths.cpp"
        src/query/dialect/pmltq/pmltq_lexer.cpp
        src/query/dialect/pmltq/pmltq_parser.cpp
        src/query/dialect/pmltq/pmltq_test_lower.cpp
        src/query/dialect/pmltq/pmltq_native_lower.cpp
        src/query/dialect/pmltq/pmltq_gold_json.cpp
        src/query/dialect/pmltq/pmltq_gold_bridge.cpp
        src/query/dialect/pmltq/pmltq_gold_lower.cpp
        src/query/dialect/pmltq/pmltq_translate.cpp
    )
else()
    add_library(pando_pmltq_dialect STATIC
        src/query/dialect/pmltq/pmltq_translate_stub.cpp
    )
endif()
target_include_directories(pando_pmltq_dialect PUBLIC src)

# Build identity (git describe) for --version, /health, /info: regenerated on
# every build, rewritten only when it changes.
set(PANDO_BUILD_INFO_CPP "${CMAKE_CURRENT_BINARY_DIR}/pando_build_info.cpp")
set(PANDO_BUILD_INFO_ARGS -DSRC=${CMAKE_CURRENT_SOURCE_DIR} -DOUT=${PANDO_BUILD_INFO_CPP}
    -DVERSION=${PROJECT_VERSION} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/build_info.cmake)
execute_process(COMMAND ${CMAKE_COMMAND} ${PANDO_BUILD_INFO_ARGS})
add_custom_target(pando_build_info
    COMMAND ${CMAKE_COMMAND} ${PANDO_BUILD_INFO_ARGS}
    BYPRODUCTS ${PANDO_BUILD_INFO_CPP}
    COMMENT "pando build identity")

# Shared library: libpando.so / libpando.dylib (#43)
# Packages pando_core + pando_api + C FFI wrappers (pando_ffi.h, server_capi.h) for
# external consumers. Enable with: cmake -DPANDO_BUILD_SHARED=ON ..
# The static libraries it bundles must then be position independent (thread-local
# storage in them cannot be linked into a shared object otherwise); embedders that
# add this tree with add_subdirectory (flexicorp_pando) set the same themselves.
option(PANDO_BUILD_SHARED "Build libpando shared library" OFF)
if(PANDO_BUILD_SHARED)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
endif()

add_library(pando_core STATIC
    ${PANDO_BUILD_INFO_CPP}
    src/core/build_info.cpp
    src/core/mmap_file.cpp
    src/core/regex_engine.cpp
    src/core/count_hierarchy_json.cpp
    src/index/lexicon.cpp
    src/index/fold_map.cpp
    src/index/fold_index.cpp
    src/index/dep_pair_index.cpp
    src/index/bitmap_index.cpp
    src/index/packed_postings.cpp
    src/index/positional_attr.cpp
    src/index/structural_attr.cpp
    src/index/dependency_index.cpp
    src/corpus/corpus.cpp
    src/corpus/builder.cpp
    src/corpus/streaming_builder.cpp
    src/query/lexer.cpp
    src/query/parser.cpp
    src/query/dialect/tiger/tiger_translate.cpp
    src/query/executor.cpp
    src/query/executor_aggregate.cpp
    src/query/executor_sort.cpp
    src/query/executor_anchors.cpp
    src/query/executor_tabulate.cpp
    src/api/index_api.cpp
)
target_include_directories(pando_core PUBLIC src)
add_dependencies(pando_core pando_build_info)
target_link_libraries(pando_core PUBLIC pando_cwb_dialect pando_pmltq_dialect)

if(PANDO_WITH_RE2)
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

add_executable(pando-check src/cli/check_main.cpp)
target_link_libraries(pando-check PRIVATE pando_core)

add_executable(cwb-import src/cli/cwb_import_main.cpp)
target_link_libraries(cwb-import PRIVATE pando_core)

# API layer: query execution + JSON (used by CLI and server)
add_library(pando_api STATIC src/api/query_json.cpp src/api/program_api.cpp src/api/query_jobs.cpp src/api/group_counts.cpp src/api/coll_counts.cpp src/api/result_cache.cpp
    src/api/server_api.cpp src/api/server_capi.cpp src/api/sessions.cpp
    src/api/limits.cpp)
target_include_directories(pando_api PUBLIC src)
find_package(Threads REQUIRED)
target_link_libraries(pando_api PUBLIC pando_core Threads::Threads)


configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/pando_version.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/pando_version.h"
    @ONLY)

# Interactive REPL: linenoise (BSD) for line editing, history, up/down arrows.
# FetchContent (first configure needs network). Disabled on Windows (fallback: std::getline).
# Upstream has no CMakeLists.txt; PATCH_COMMAND copies ours so only MakeAvailable is needed (CMP0169).
option(PANDO_LINENOISE_REPL "Link linenoise for interactive pando REPL" ON)
if(PANDO_LINENOISE_REPL AND NOT WIN32)
    FetchContent_Declare(linenoise_fc
        GIT_REPOSITORY https://github.com/antirez/linenoise.git
        GIT_TAG 452e3793858a15cc006bb3aa3ea06378eb98c5b5
    )
    # Upstream linenoise ships no CMakeLists.txt; instead of patching the fetched tree
    # (which can be brittle in containerized environments), we populate and wire it up
    # via an explicit target.
    FetchContent_GetProperties(linenoise_fc)
    if(NOT linenoise_fc_POPULATED)
        FetchContent_Populate(linenoise_fc)
    endif()
    add_library(pando_linenoise STATIC "${linenoise_fc_SOURCE_DIR}/linenoise.c")
    target_include_directories(pando_linenoise PUBLIC "${linenoise_fc_SOURCE_DIR}")
    set_target_properties(pando_linenoise PROPERTIES C_STANDARD 99)
endif()

# CLI executable
add_executable(pando src/cli/query_main.cpp)
target_include_directories(pando PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(pando PRIVATE pando_api)
if(TARGET pando_linenoise)
    target_link_libraries(pando PRIVATE pando_linenoise)
    target_compile_definitions(pando PRIVATE PANDO_HAVE_LINENOISE=1)
endif()
if(PANDO_CWB_DIALECT)
    target_compile_definitions(pando PRIVATE PANDO_WITH_CWB_DIALECT=1)
endif()

if(BUILD_TESTING AND PANDO_CWB_DIALECT)
    add_executable(cwb_dialect_test src/query/dialect/cwb/cwb_dialect_test.cpp)
    target_link_libraries(cwb_dialect_test PRIVATE pando_core)
    add_test(NAME cwb_dialect COMMAND cwb_dialect_test)
endif()

if(BUILD_TESTING AND PANDO_PMLTQ_DIALECT)
    add_executable(pmltq_dialect_test src/query/dialect/pmltq/pmltq_dialect_test.cpp)
    target_link_libraries(pmltq_dialect_test PRIVATE pando_pmltq_dialect)
    add_test(NAME pmltq_dialect COMMAND pmltq_dialect_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endif()

if(BUILD_TESTING)
    add_executable(tiger_dialect_test src/query/dialect/tiger/tiger_dialect_test.cpp)
    target_link_libraries(tiger_dialect_test PRIVATE pando_core)
    add_test(NAME tiger_dialect COMMAND tiger_dialect_test)
    add_executable(parser_quoted_test src/query/parser_quoted_test.cpp)
    target_link_libraries(parser_quoted_test PRIVATE pando_core)
    add_test(NAME parser_quoted COMMAND parser_quoted_test)
    add_executable(mv_fwd_test src/index/mv_fwd_test.cpp)
    target_link_libraries(mv_fwd_test PRIVATE pando_core)
    add_test(NAME mv_fwd COMMAND mv_fwd_test)

    add_executable(mv_stage1_executor_test src/index/mv_stage1_executor_test.cpp)
    target_link_libraries(mv_stage1_executor_test PRIVATE pando_core)
    add_test(NAME mv_stage1_executor COMMAND mv_stage1_executor_test)

    add_executable(bitmap_index_test src/index/bitmap_index_test.cpp)
    target_link_libraries(bitmap_index_test PRIVATE pando_core)
    add_test(NAME bitmap_index COMMAND bitmap_index_test)

    add_executable(group_counts_test src/api/group_counts_test.cpp)
    target_link_libraries(group_counts_test PRIVATE pando_api)
    add_test(NAME group_counts COMMAND group_counts_test)
    add_executable(server_api_test src/api/server_api_test.cpp)
    target_link_libraries(server_api_test PRIVATE pando_api)
    add_test(NAME server_api COMMAND server_api_test $<TARGET_FILE:pando-index>
             ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample.conllu)

    # Differential test: fast paths (S1 merge, dep bitset, seq probe) must return exactly
    # the same matches and totals as the generic executor (PANDO_FASTPATH=on|nomerge|off).
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND)
        add_test(NAME fastpath_diff
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/fastpath_diff.py
                --pando $<TARGET_FILE:pando> --pando-index $<TARGET_FILE:pando-index>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample.conllu
                --queries ${CMAKE_CURRENT_SOURCE_DIR}/test/fastpath_queries.txt)
        # Persistent names: alignment with a token named by an earlier statement
        add_test(NAME persistent_names
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/persistent_names_test.py
                --pando $<TARGET_FILE:pando> --pando-index $<TARGET_FILE:pando-index>
                --server $<TARGET_FILE:pando-server>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/aligned.conllu)
        # a bare "string" without contr_form = one token (CQP)
        add_test(NAME bare_string
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/bare_string_test.py
                --pando $<TARGET_FILE:pando> --pando-index $<TARGET_FILE:pando-index>)
        # P6.4: the server's result cache changes no answer (aligned: persistent names)
        add_test(NAME result_cache
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/result_cache_test.py
                --server $<TARGET_FILE:pando-server> --pando-index $<TARGET_FILE:pando-index>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/aligned.conllu)
        # P7.8: coll / dcoll via the hit sink and page-only tabulate = over all hits
        add_test(NAME coll_sink
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/coll_sink_test.py
                --pando $<TARGET_FILE:pando> --pando-index $<TARGET_FILE:pando-index>
                --server $<TARGET_FILE:pando-server>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample.conllu)
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/src/index/dep_jsonl_alignment_test.cpp")
        add_executable(dep_jsonl_alignment_test src/index/dep_jsonl_alignment_test.cpp)
        target_link_libraries(dep_jsonl_alignment_test PRIVATE pando_core)
        add_test(NAME dep_jsonl_alignment COMMAND dep_jsonl_alignment_test)
    else()
        message(STATUS "Skipping dep_jsonl_alignment test; missing src/index/dep_jsonl_alignment_test.cpp")
    endif()
endif()

install(TARGETS pando_cwb_dialect
    ARCHIVE DESTINATION lib
    COMPONENT cwb_dialect)
install(TARGETS pando_pmltq_dialect
    ARCHIVE DESTINATION lib
    COMPONENT pmltq_dialect)

if(PANDO_BUILD_SHARED)
    add_library(libpando SHARED src/api/pando_ffi.cpp src/api/server_capi.cpp)
    set_target_properties(libpando PROPERTIES
        OUTPUT_NAME "pando"           # produces libpando.so / libpando.dylib
        VERSION ${PROJECT_VERSION}
        SOVERSION 0
        # Hide all symbols by default; only the C API is exported
        C_VISIBILITY_PRESET hidden
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
    )
    target_link_libraries(libpando PRIVATE pando_api)
    target_compile_definitions(libpando PRIVATE PANDO_BUILDING_SHARED)
    install(TARGETS libpando LIBRARY DESTINATION lib)
    install(FILES src/api/pando_ffi.h src/api/server_capi.h DESTINATION include)
endif()

# HTTP API server (optional: requires FetchContent to get cpp-httplib)
add_executable(pando-server src/cli/server_main.cpp)
target_include_directories(pando-server PRIVATE ${httplib_SOURCE_DIR})
target_link_libraries(pando-server PRIVATE pando_api)

if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND)
        # P6.2: background totals, result cache, cancel / abandon (/query "total": "async")
        add_test(NAME server_jobs
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/server_jobs_test.py
                --server $<TARGET_FILE:pando-server> --pando $<TARGET_FILE:pando>
                --pando-index $<TARGET_FILE:pando-index>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample.conllu)
        # limits by tier ("tier" per request, --limits FILE, --trust-tier)
        add_test(NAME server_limits
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/server_limits_test.py
                --server $<TARGET_FILE:pando-server> --pando-index $<TARGET_FILE:pando-index>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample.conllu)
        # /query "sample" / "shuffle" / "seed" (KonText random sample and shuffle)
        add_test(NAME server_sample
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/server_sample_test.py
                --server $<TARGET_FILE:pando-server> --pando-index $<TARGET_FILE:pando-index>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample.conllu)
        # P6.1: client sessions (stored hit sets, sort / count / pages without re-running)
        add_test(NAME server_sessions
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test/server_sessions_test.py
                --server $<TARGET_FILE:pando-server> --pando-index $<TARGET_FILE:pando-index>
                --conllu ${CMAKE_CURRENT_SOURCE_DIR}/test/data/sample.conllu)
    endif()
endif()

# `cmake --install build [--prefix DIR]` (or ./install.sh): the commands, and the UD
# corpus builder as `pando-build-ud`
install(TARGETS pando pando-index pando-check pando-server RUNTIME DESTINATION bin)
install(PROGRAMS scripts/build_ud_corpus.py DESTINATION bin RENAME pando-build-ud)
