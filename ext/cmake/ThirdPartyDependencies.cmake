add_subdirectory(third_party/gsl)

option(TAOCPP_JSON_BUILD_TESTS OFF)
option(TAOCPP_JSON_BUILD_EXAMPLES OFF)
add_subdirectory(third_party/json)

add_subdirectory(third_party/spdlog)

option(SNAPPY_BUILD_TESTS OFF)
option(SNAPPY_INSTALL OFF)
add_subdirectory(third_party/snappy)

option(HDR_LOG_REQUIRED OFF)
option(HDR_HISTOGRAM_BUILD_SHARED OFF)
option(HDR_HISTOGRAM_BUILD_PROGRAMS OFF)
add_subdirectory(third_party/hdr_histogram_c)
include_directories(BEFORE SYSTEM third_party/hdr_histogram_c/src)

include_directories(BEFORE SYSTEM third_party/gsl/include)
include_directories(BEFORE SYSTEM third_party/asio/asio/include)
include_directories(BEFORE SYSTEM third_party/json/include)
include_directories(BEFORE SYSTEM third_party/json/external/PEGTL/include)
include_directories(BEFORE SYSTEM third_party/spdlog/include)
include_directories(BEFORE SYSTEM third_party/http_parser)

add_library(http_parser OBJECT third_party/http_parser/http_parser.c)
set_target_properties(http_parser PROPERTIES C_VISIBILITY_PRESET hidden POSITION_INDEPENDENT_CODE TRUE)
