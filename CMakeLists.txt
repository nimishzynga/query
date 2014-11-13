PROJECT(Memcached)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

INCLUDE(FindCouchbaseLibevent.cmake)

SET(Platform_SOURCE_DIR "/Users/nimishgupta/new_couchbase/platform")
SET(Platform_LIB_DIR "/Users/nimishgupta/new_couchbase/install/lib/libplatform.0.1.0.dylib")

INCLUDE_DIRECTORIES(BEFORE
    ${LIBEVENT_INCLUDE_DIR},
    ${Platform_SOURCE_DIR}/include)

ADD_EXECUTABLE(query
    task.c
    http_parser.c)

TARGET_LINK_LIBRARIES(query ${LIBEVENT_LIBRARIES} ${Platform_LIB_DIR})
