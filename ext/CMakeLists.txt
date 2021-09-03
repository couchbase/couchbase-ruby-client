cmake_minimum_required(VERSION 3.15)
project(ruby_client LANGUAGES CXX)

if(RUBY_HDR_DIR)
  set(RUBY_INCLUDE_DIR ${RUBY_HDR_DIR} ${RUBY_ARCH_HDR_DIR})
else()
  find_package(Ruby 2.6.0 REQUIRED)
  message(STATUS "RUBY_VERSION: ${RUBY_VERSION}")
  message(STATUS "RUBY_EXECUTABLE: ${RUBY_EXECUTABLE}")
endif()
message(STATUS "RUBY_LIBRARY: ${RUBY_LIBRARY}")
if(RUBY_INCLUDE_DIR)
  message(STATUS "RUBY_INCLUDE_DIR: ${RUBY_INCLUDE_DIR}")
  include_directories(BEFORE SYSTEM "${RUBY_INCLUDE_DIR}")
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
constexpr auto RUBY_LIBRARY = \"${RUBY_LIBRARY}\";
constexpr auto RUBY_INCLUDE_DIR = \"${RUBY_INCLUDE_DIR}\";
")

add_library(couchbase SHARED couchbase.cxx)
target_include_directories(couchbase PRIVATE ${PROJECT_BINARY_DIR}/generated)
target_link_libraries(couchbase PRIVATE project_options project_warnings couchbase_cxx_client)

if(APPLE)
  target_link_libraries(couchbase PRIVATE -Wl,-undefined,dynamic_lookup)
endif()
