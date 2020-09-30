cmake_minimum_required(VERSION 3.15)
project(ruby_client LANGUAGES CXX C)
include(cmake/PreventInSourceBuilds.cmake)
include(cmake/StandardProjectSettings.cmake)

# 'library' to set the c++ standard / compile-time options requested
add_library(project_options INTERFACE)
target_compile_features(project_options INTERFACE cxx_std_17)

# 'library' to use the warnings specified in CompilerWarnings.cmake
add_library(project_warnings INTERFACE)

include(cmake/BuildTracing.cmake)

# enable cache system
include(cmake/Cache.cmake)

# standard compiler warnings
include(cmake/CompilerWarnings.cmake)
set_project_warnings(project_warnings)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)
enable_sanitizers(project_options)

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)

include(cmake/Backtrace.cmake)

include(cmake/ThirdPartyDependencies.cmake)

option(STATIC_STDLIB "Statically link C++ standard library" FALSE)
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if(STATIC_STDLIB)
    target_compile_options(project_options INTERFACE -static-libgcc -static-libstdc++)
    target_link_libraries(project_options INTERFACE -static-libgcc -static-libstdc++)
  endif()
endif()

option(STATIC_OPENSSL "Statically link OpenSSL library" FALSE)
# Read more at https://wiki.wireshark.org/TLS
option(TLS_KEY_LOG_FILE "Path to file to write per-session secrets (Useful for Wireshark SSL/TLS dissection)")
if(STATIC_OPENSSL)
  set(OPENSSL_USE_STATIC_LIBS ON)
endif()
include(FindOpenSSL)
message(STATUS "OPENSSL_VERSION: ${OPENSSL_VERSION}")
message(STATUS "OPENSSL_INCLUDEDIRS: ${OPENSSL_INCLUDE_DIR}")
message(STATUS "OPENSSL_LIBRARIES: ${OPENSSL_LIBRARIES}")

include(cmake/VersionInfo.cmake)

include_directories(${CMAKE_SOURCE_DIR}/couchbase)

add_library(
  platform OBJECT
  couchbase/platform/string_hex.cc
  couchbase/platform/uuid.cc
  couchbase/platform/random.cc
  couchbase/platform/base64.cc
  couchbase/platform/backtrace.c
  couchbase/platform/terminate_handler.cc)
target_include_directories(platform PRIVATE ${PROJECT_BINARY_DIR}/generated)
target_link_libraries(platform PRIVATE ${PLATFORM_LIBRARIES})

add_library(cbcrypto OBJECT couchbase/cbcrypto/cbcrypto.cc)
target_link_libraries(
  cbcrypto
  PRIVATE project_options
          project_warnings
          OpenSSL::SSL
          OpenSSL::Crypto)

add_library(
  cbsasl OBJECT
  couchbase/cbsasl/client.cc
  couchbase/cbsasl/context.cc
  couchbase/cbsasl/mechanism.cc
  couchbase/cbsasl/plain/plain.cc
  couchbase/cbsasl/scram-sha/scram-sha.cc
  couchbase/cbsasl/scram-sha/stringutils.cc)

if(RUBY_HDR_DIR)
  set(RUBY_INCLUDE_DIR ${RUBY_HDR_DIR} ${RUBY_ARCH_HDR_DIR})
else()
  find_package(Ruby REQUIRED)
  message(STATUS "RUBY_VERSION: ${RUBY_VERSION}")
  message(STATUS "RUBY_EXECUTABLE: ${RUBY_EXECUTABLE}")
endif()
message(STATUS "RUBY_INCLUDE_DIR: ${RUBY_INCLUDE_DIR}")
message(STATUS "RUBY_LIBRARY: ${RUBY_LIBRARY}")
include_directories(BEFORE SYSTEM "${RUBY_INCLUDE_DIR}")

add_library(couchbase SHARED couchbase/couchbase.cxx)
target_include_directories(couchbase PRIVATE ${PROJECT_BINARY_DIR}/generated)
target_link_libraries(
  couchbase
  PRIVATE project_options
          project_warnings
          OpenSSL::SSL
          OpenSSL::Crypto
          platform
          cbcrypto
          cbsasl
          http_parser
          snappy
          spdlog::spdlog_header_only)

set_target_properties(
  cbcrypto
  cbsasl
  platform
  snappy
  PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

if(APPLE)
  target_link_libraries(couchbase PRIVATE -Wl,-undefined,dynamic_lookup)
else()
  set_target_properties(cbcrypto cbsasl platform PROPERTIES CXX_VISIBILITY_PRESET hidden)
endif()

include(cmake/Testing.cmake)
