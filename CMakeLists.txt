cmake_minimum_required(VERSION 3.16)
project(sqlEngine VERSION 0.2.0 LANGUAGES CXX)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
endif()

if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
endif()

# Build options
option(BUILD_TESTS "Build tests" ON)
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

# Source directories
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Collect all source files
file(GLOB_RECURSE ALL_SOURCES "${SRC_DIR}/*.cpp")

# Separate main.cpp from other sources
set(CORE_SOURCES ${ALL_SOURCES})
list(FILTER CORE_SOURCES EXCLUDE REGEX "src/main\\.cpp$")

# Create core library
add_library(sqlEngineCore STATIC ${CORE_SOURCES})
target_include_directories(sqlEngineCore PUBLIC ${INCLUDE_DIR})

# Create main executable
add_executable(sqlEngine src/main.cpp)
target_link_libraries(sqlEngine PRIVATE sqlEngineCore)

# Find and link required libraries
find_package(Threads REQUIRED)
target_link_libraries(sqlEngineCore PUBLIC Threads::Threads)

# System libraries for networking
if(UNIX AND NOT APPLE)
    target_link_libraries(sqlEngineCore PUBLIC rt)
endif()

# Build test executable
if(BUILD_TESTS)
    # Create test executable
    add_executable(sqlEngine_tests tests/cloudSQL_tests.cpp)
    target_link_libraries(sqlEngine_tests PRIVATE sqlEngineCore)
    
    # Check if GoogleTest is available
    find_package(GTest QUIET)
    if(GTest_FOUND)
        target_link_libraries(sqlEngine_tests PRIVATE GTest::GTest GTest::GTestMain)
        include(GoogleTest)
        gtest_discover_tests(sqlEngine_tests)
    endif()
    
    add_custom_target(run-tests
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/sqlEngine_tests
        DEPENDS sqlEngine_tests
        COMMENT "Running cloudSQL tests"
    )
endif()

# Installation
install(TARGETS sqlEngine RUNTIME DESTINATION bin)
install(DIRECTORY ${INCLUDE_DIR} DESTINATION include)

# Print configuration
message(STATUS "=============================================")
message(STATUS "cloudSQL Build Configuration")
message(STATUS "=============================================")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "  Build Tests: ${BUILD_TESTS}")
message(STATUS "=============================================")
