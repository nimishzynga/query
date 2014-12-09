PROJECT(Memcached)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

INCLUDE(FindCouchbaseLibevent.cmake)

SET(Platform_SOURCE_DIR "/Users/nimishgupta/new_couchbase/platform")
SET(Platform_LIB_DIR "/Users/nimishgupta/new_couchbase/install/lib/libplatform.0.1.0.dylib")

INCLUDE_DIRECTORIES(BEFORE
    ${LIBEVENT_INCLUDE_DIR},
    ${Platform_SOURCE_DIR}/include)

add_compile_options(-g)
SET(GCC_COVERAGE_COMPILE_FLAGS "-g")
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}" )

ADD_EXECUTABLE(query
    task.cc
    json_parse.cc
    jsmn.c
    http_parser.c
    test_parser.c)

TARGET_LINK_LIBRARIES(query ${LIBEVENT_LIBRARIES} ${Platform_LIB_DIR})
