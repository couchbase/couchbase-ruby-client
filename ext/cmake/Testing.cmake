if(ENABLE_TESTING)
  file(
    GENERATE
    OUTPUT ${PROJECT_BINARY_DIR}/generated/test_config.hxx
    CONTENT
      "
#pragma once
#define LIBCOUCHBASE_EXT_PATH \"$<TARGET_FILE:couchbase>\"
")

  add_subdirectory(third_party/catch2)
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/third_party/catch2/contrib")
  enable_testing()
  include(Catch)

  macro(ruby_test name)
    add_executable(test_ruby_${name} test/test_ruby_${name}.cxx)
    target_include_directories(test_ruby_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
    target_link_libraries(
      test_ruby_${name}
      project_options
      project_warnings
      ${RUBY_LIBRARY}
      couchbase
      Catch2::Catch2)
    catch_discover_tests(test_ruby_${name})
  endmacro()

  macro(native_test name)
    add_executable(test_native_${name} test/test_native_${name}.cxx)
    target_include_directories(test_native_${name} PRIVATE ${PROJECT_BINARY_DIR}/generated)
    target_link_libraries(
      test_native_${name}
      project_options
      project_warnings
      Catch2::Catch2
      OpenSSL::SSL
      OpenSSL::Crypto
      platform
      cbcrypto
      cbsasl
      http_parser
      snappy
      spdlog::spdlog_header_only)
    catch_discover_tests(test_native_${name})
  endmacro()

  ruby_test(trivial_crud)
  ruby_test(trivial_query)
  native_test(trivial_crud)
  native_test(diagnostics)
endif()
