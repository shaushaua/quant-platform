cmake_minimum_required(VERSION 3.16)
project(native_mdl_collector LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ── MDL SDK paths (override with -DMDL_SDK_ROOT=...) ──
set(MDL_SDK_ROOT "${CMAKE_SOURCE_DIR}/../vendor/mdl-sdk" CACHE PATH "MDL C++ SDK root")

# ── Compiler flags ──
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -DDEBUG")

add_executable(native-mdl-collector
    src/main.cpp
    src/shm_writer.cpp
    src/mdl_handler.cpp
    src/parser_sh.cpp
    src/parser_sz.cpp
    src/metrics.cpp
)

target_include_directories(native-mdl-collector PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${MDL_SDK_ROOT}/include
    ${MDL_SDK_ROOT}/3rd
)

target_link_directories(native-mdl-collector PRIVATE
    ${MDL_SDK_ROOT}/libs/linux
)

target_link_libraries(native-mdl-collector PRIVATE
    mdl_api
    dl
    pthread
)

# librt is Linux-only (clock_gettime, etc.)
if(UNIX AND NOT APPLE)
    target_link_libraries(native-mdl-collector PRIVATE rt)
endif()

# Set RPATH so the binary finds libmdl_api.so at runtime
set_target_properties(native-mdl-collector PROPERTIES
    BUILD_RPATH "${MDL_SDK_ROOT}/libs/linux"
    INSTALL_RPATH "$ORIGIN/../lib"
)
