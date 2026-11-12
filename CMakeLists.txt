cmake_minimum_required(VERSION 3.13)

# Set build type to Benchmark if not specified
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Benchmark)
endif()

# Compiler picked up from PATH. Load the desired toolchain before running
# cmake (e.g. `module load gcc/13.3.0`). Override with -DCMAKE_CXX_COMPILER=...
# if you want to pin a specific binary.

# Ask every target in this project to compile as C++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)   # fail if the compiler can't do C++20
set(CMAKE_CXX_EXTENSIONS  OFF)        # use -std=c++20, not gnu++20

# Automatically enables all available optimizations for your local machine, including AVX2 and AVX512
set(CMAKE_CXX_FLAGS "-O3 -MD -march=native -mtune=native")  # Enable all optimizations

# Add perf when building in Debug mode
if (CMAKE_BUILD_TYPE MATCHES Debug)
    set(CMAKE_CXX_FLAGS "-O2 -MD -pg -march=native -mtune=native")  # Enable profiling flags
endif()


# Option to enable Intel HEXL optimizations. Default is ON.
option(USE_HEXL "Enable Intel HEXL optimizations" ON)
if (USE_HEXL)
    message(STATUS "HEXL support enabled.")
    add_compile_definitions(ONIONPIR_USE_HEXL)
    
    # Set HEXL installation path (override with -DHEXL_DIR=... or the HEXL_DIR env var)
    if (NOT HEXL_DIR AND DEFINED ENV{HEXL_DIR})
        set(HEXL_DIR "$ENV{HEXL_DIR}")
    endif()
    if (NOT HEXL_DIR)
        set(HEXL_DIR "/u/yuec12/OnionPIRv2/hexl_install/lib64/cmake/hexl-1.2.6")
    endif()
    
    # Find HEXL package
    find_package(HEXL REQUIRED)
    if(HEXL_FOUND)
        message(STATUS "HEXL found at: ${HEXL_DIR}")
    else()
        message(FATAL_ERROR "HEXL not found. Please install HEXL or disable USE_HEXL option.")
    endif()
else()
    message(STATUS "HEXL support disabled.")
endif()


option(NOAVX512 "Disable AVX-512" OFF)
option(NOAVX2 "Disable AVX2" OFF)
if (NOAVX512)
    message("Disabling AVX-512")
    add_compile_options(-mno-avx512f)
endif()
if (NOAVX2)
    message("Disabling AVX2")
    add_compile_options(-mno-avx2)
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -D_DEBUG")
set(IS_BENCHMARK_BUILD CMAKE_BUILD_TYPE STREQUAL "Benchmark")
if (${IS_BENCHMARK_BUILD})
    add_compile_definitions(_BENCHMARK)
endif ()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
project(Onion-PIR)

# Build-time selector for the parameter set.
# Override on the cmake line:
#   cmake -DACTIVE_CONFIG=CONFIG_N2048_K1_COMP ..
# Default is picked up from src/includes/database_constants.h when unset.
if (DEFINED ACTIVE_CONFIG)
    message(STATUS "ACTIVE_CONFIG = ${ACTIVE_CONFIG}")
    add_compile_definitions(ACTIVE_CONFIG=${ACTIVE_CONFIG})
endif()


find_package(Threads REQUIRED)

# ---- Core library: everything directly in src/ except the test driver.
file(GLOB CORE_SRC ${CMAKE_SOURCE_DIR}/src/*.cpp)
list(REMOVE_ITEM CORE_SRC ${CMAKE_SOURCE_DIR}/src/main.cpp)
add_library(onionpir_core STATIC ${CORE_SRC})
target_include_directories(onionpir_core PUBLIC src/includes)
target_link_libraries(onionpir_core PUBLIC Threads::Threads)
if (USE_HEXL AND HEXL_FOUND)
    target_link_libraries(onionpir_core PUBLIC HEXL::hexl)
endif()

# ---- Single-query benchmark / test driver (python3 run.py -t ...).
file(GLOB TEST_SRC ${CMAKE_SOURCE_DIR}/src/tests/*.cpp)
add_executable(Onion-PIR src/main.cpp ${TEST_SRC})
target_link_libraries(Onion-PIR PRIVATE onionpir_core)

# ---- Multi-client server/client over a streamed, uncooperative database.
# The database is the Zealot/shuffle Go service; dbbridge/ is a cgo c-archive
# that speaks its gRPC API using shuffle's own generated stubs.
option(BUILD_STREAMING "Build onion_server/onion_client (needs Go and a shuffle checkout)" ON)
set(SHUFFLE_DIR "${CMAKE_SOURCE_DIR}/../shuffle" CACHE PATH
    "Zealot/shuffle checkout that provides the database API (proto_gen)")
if (BUILD_STREAMING)
    find_program(GO_EXECUTABLE go HINTS /usr/local/go/bin $ENV{HOME}/go/bin)
    get_filename_component(SHUFFLE_DIR_ABS "${SHUFFLE_DIR}" ABSOLUTE)
    if (NOT GO_EXECUTABLE)
        message(WARNING "Go not found: skipping onion_server/onion_client (install Go 1.25+).")
        set(BUILD_STREAMING OFF)
    elseif (NOT EXISTS "${SHUFFLE_DIR_ABS}/proto_gen/pir_grpc.pb.go")
        message(WARNING "${SHUFFLE_DIR_ABS}/proto_gen not found: skipping onion_server/onion_client. "
                        "Run `python3 proto.py` in the shuffle checkout, or set -DSHUFFLE_DIR=...")
        set(BUILD_STREAMING OFF)
    endif()
endif()
if (BUILD_STREAMING)
    message(STATUS "Database API from shuffle checkout: ${SHUFFLE_DIR_ABS}")
    if (NOT "${ACTIVE_CONFIG}" STREQUAL "CONFIG_N2048_K1_COMP")
        message(STATUS "Note: onion_server supports only ACTIVE_CONFIG=CONFIG_N2048_K1_COMP (it exits otherwise).")
    endif()

    # Build-dir copy of go.mod whose replace directive points at SHUFFLE_DIR,
    # so the source tree is never edited.
    set(BRIDGE_DIR ${CMAKE_BINARY_DIR}/dbbridge)
    file(MAKE_DIRECTORY ${BRIDGE_DIR})
    file(READ ${CMAKE_SOURCE_DIR}/dbbridge/go.mod BRIDGE_GOMOD)
    string(REPLACE "=> ../../shuffle" "=> ${SHUFFLE_DIR_ABS}" BRIDGE_GOMOD "${BRIDGE_GOMOD}")
    file(WRITE ${BRIDGE_DIR}/go.mod.in "${BRIDGE_GOMOD}")
    configure_file(${BRIDGE_DIR}/go.mod.in ${BRIDGE_DIR}/go.mod COPYONLY)
    configure_file(${CMAKE_SOURCE_DIR}/dbbridge/go.sum ${BRIDGE_DIR}/go.sum COPYONLY)

    # Always invoke go build; Go's own cache makes it a no-op when nothing
    # (here or in the shuffle checkout) changed.
    add_custom_target(dbbridge
        COMMAND ${CMAKE_COMMAND} -E env CGO_ENABLED=1
                ${GO_EXECUTABLE} build -buildvcs=false -modfile=${BRIDGE_DIR}/go.mod
                -buildmode=c-archive -o ${BRIDGE_DIR}/libdbbridge.a .
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/dbbridge
        BYPRODUCTS ${BRIDGE_DIR}/libdbbridge.a ${BRIDGE_DIR}/libdbbridge.h
        COMMENT "Building Go database bridge (cgo c-archive)")

    file(GLOB STREAM_SRC ${CMAKE_SOURCE_DIR}/src/stream/*.cpp)
    add_library(onionpir_stream STATIC ${STREAM_SRC})
    add_dependencies(onionpir_stream dbbridge)
    target_include_directories(onionpir_stream PUBLIC src/stream ${BRIDGE_DIR})
    target_link_libraries(onionpir_stream PUBLIC onionpir_core ${BRIDGE_DIR}/libdbbridge.a)
    # System libraries the Go runtime (net, crypto/x509) needs in a c-archive.
    if (APPLE)
        target_link_libraries(onionpir_stream PUBLIC resolv
            "-framework CoreFoundation" "-framework Security")
    else()
        target_link_libraries(onionpir_stream PUBLIC resolv ${CMAKE_DL_LIBS})
    endif()

    add_executable(onion_server src/apps/onion_server.cpp)
    target_link_libraries(onion_server PRIVATE onionpir_stream)
    add_executable(onion_client src/apps/onion_client.cpp)
    target_link_libraries(onion_client PRIVATE onionpir_stream)
endif()
