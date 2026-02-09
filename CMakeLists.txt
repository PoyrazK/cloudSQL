cmake_minimum_required(VERSION 3.16)
project(sqlEngine VERSION 0.2.0 LANGUAGES CXX)

# C Standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# C++ Standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")
endif()

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")
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

# Collect source files (C++ only for now)
file(GLOB_RECURSE SOURCES_CXX "${SRC_DIR}/*.cpp")
file(GLOB_RECURSE HEADERS_HPP "${INCLUDE_DIR}/*.hpp")

# Create main executable (C++ only - using test file as source for header-only libs)
add_executable(sqlEngine
    tests/cloudSQL_tests.cpp
    ${SOURCES_CXX}
    ${HEADERS_HPP}
)

# Include directories
target_include_directories(sqlEngine PRIVATE
    ${INCLUDE_DIR}
)

# Find and link required libraries
find_package(Threads REQUIRED)

target_link_libraries(sqlEngine PRIVATE
    Threads::Threads
)

# System libraries for networking
if(UNIX AND NOT APPLE)
    target_link_libraries(sqlEngine PRIVATE
        rt
    )
endif()

# Build simple test executable (always available)
if(BUILD_TESTS)
    # Check if GoogleTest is available
    find_package(GTest QUIET)
    
    if(GTest_FOUND)
        message(STATUS "GoogleTest found - building GTest suite")
        
        # Create test executable with GoogleTest
        add_executable(sqlEngine_tests tests/cloudSQL_tests.cpp)
        target_include_directories(sqlEngine_tests PRIVATE ${INCLUDE_DIR})
        target_link_libraries(sqlEngine_tests PRIVATE
            GTest::GTest
            GTest::GTestMain
            Threads::Threads
        )
        include(GoogleTest)
        gtest_discover_tests(sqlEngine_tests)
        
        add_custom_target(run-gtests
            COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R sqlEngine_tests
            DEPENDS sqlEngine_tests
            COMMENT "Running GoogleTest suite"
        )
    else()
        message(STATUS "GoogleTest not found - using manual test runner")
        
        # Build simple test executable
        add_executable(sqlEngine_tests tests/cloudSQL_tests.cpp)
        target_include_directories(sqlEngine_tests PRIVATE ${INCLUDE_DIR})
        target_link_libraries(sqlEngine_tests PRIVATE Threads::Threads)
        
        add_custom_target(run-tests
            COMMAND ${CMAKE_CURRENT_BINARY_DIR}/sqlEngine_tests
            DEPENDS sqlEngine_tests
            COMMENT "Running cloudSQL tests"
        )
    endif()
endif()

# Installation
install(TARGETS sqlEngine RUNTIME DESTINATION bin)
install(DIRECTORY ${INCLUDE_DIR} DESTINATION include)

# Print configuration
message(STATUS "=============================================")
message(STATUS "cloudSQL Build Configuration")
message(STATUS "=============================================")
message(STATUS "  Version: ${PROJECT_VERSION}")
message(STATUS "  C Standard: ${CMAKE_C_STANDARD}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Compiler: ${CMAKE_C_COMPILER_ID} / ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "  Build Tests: ${BUILD_TESTS}")
message(STATUS "=============================================")
