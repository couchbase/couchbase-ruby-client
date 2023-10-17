cmake_minimum_required(VERSION 3.20)
project(ruby_client LANGUAGES CXX)

if(RUBY_HDR_DIR)
  set(RUBY_INCLUDE_DIR ${RUBY_HDR_DIR} ${RUBY_ARCH_HDR_DIR})
else()
  find_package(Ruby 3.0.0 REQUIRED)
  message(STATUS "RUBY_VERSION: ${RUBY_VERSION}")
  message(STATUS "RUBY_EXECUTABLE: ${RUBY_EXECUTABLE}")
endif()

add_subdirectory(couchbase)

if(NOT EXT_GIT_REVISION)
  find_program(GIT git)
  if(GIT)
    execute_process(
      COMMAND git rev-parse HEAD
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
      OUTPUT_STRIP_TRAILING_WHITESPACE
      OUTPUT_VARIABLE EXT_GIT_REVISION)
  else()
    set(EXT_GIT_REVISION "unknown")
  endif()
endif()
configure_file(${PROJECT_SOURCE_DIR}/ext_build_version.hxx.in ${PROJECT_BINARY_DIR}/generated/ext_build_version.hxx
               @ONLY)
file(
  GENERATE
  OUTPUT ${PROJECT_BINARY_DIR}/generated/ext_build_info.hxx
  CONTENT
    "
#pragma once
constexpr auto RUBY_LIBRUBYARG = \"${RUBY_LIBRUBYARG}\";
constexpr auto RUBY_INCLUDE_DIR = \"${RUBY_INCLUDE_DIR}\";
constexpr auto RUBY_LIBRARY_DIR = \"${RUBY_LIBRARY_DIR}\";
")

message(STATUS "RUBY_LIBRUBYARG: ${RUBY_LIBRUBYARG}")
message(STATUS "RUBY_INCLUDE_DIR: ${RUBY_INCLUDE_DIR}")
message(STATUS "RUBY_LIBRARY_DIR: ${RUBY_LIBRARY_DIR}")

add_library(couchbase SHARED couchbase.cxx)
target_include_directories(couchbase PRIVATE ${PROJECT_BINARY_DIR}/generated ${RUBY_INCLUDE_DIR})
target_link_libraries(
  couchbase
  PRIVATE project_options
          project_warnings
          couchbase_cxx_client
          Microsoft.GSL::GSL
          asio
          taocpp::json
          snappy)
if(RUBY_LIBRUBY)
  target_link_directories(couchbase PRIVATE "${RUBY_LIBRARY_DIR}")
  target_link_libraries(couchbase PRIVATE "${RUBY_LIBRUBY}")
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
  target_compile_options(couchbase PRIVATE -fdeclspec)
endif()

if(APPLE)
  target_link_libraries(couchbase PRIVATE -Wl,-undefined,dynamic_lookup)
endif()
