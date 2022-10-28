cmake_minimum_required(VERSION 2.5.0)

if(COMMAND cmake_policy)
  cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

project(BellportExamples)

get_filename_component(DEFAULT_BELLPORT_ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
set(BELLPORT_ROOT_DIR ${DEFAULT_BELLPORT_ROOT_DIR}  CACHE PATH  "bellport root directory")

add_definitions(-DBP_BOOST_ALL_NO_LIB)

set(CMAKE_MODULE_PATH
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${DEFAULT_BELLPORT_ROOT_DIR}/cmake"
  "${DEFAULT_BELLPORT_ROOT_DIR}/cmake/modules"
  ${CMAKE_MODULE_PATH}
  )

find_package(Bellport)

if(IS_DIRECTORY "/opt/napatech3/include" AND IS_DIRECTORY "/opt/napatech3/lib" AND NOT NAPATECH_ROOT)
  set(NAPATECH_ROOT "/opt/napatech3")
endif()
if(NAPATECH_ROOT)
  set(BELLPORT_INCLUDE_DIR ${BELLPORT_INCLUDE_DIR} "${NAPATECH_ROOT}/include")
  set(BELLPORT_THIRDPARTY_LIBRARIES ${BELLPORT_THIRDPARTY_LIBRARIES} "${NAPATECH_ROOT}/lib/libntapi.so" "${NAPATECH_ROOT}/lib/libntos.so" "${NAPATECH_ROOT}/lib/libntutil.so")
endif()

if (ELEKTRON_ROOT)
   set(BELLPORT_INCLUDE_DIR ${BELLPORT_INCLUDE_DIR} ${ELEKTRON_ROOT}/Ema/Src/)
   set(BELLPORT_THIRDPARTY_LIBRARIES ${BELLPORT_THIRDPARTY_LIBRARIES} ${ELEKTRON_ROOT}Ema/Libs/RHEL6_64_GCC444/Optimized/libema.a)
   add_definitions(-DELEKTRON_COMPILE_EXAMPLE)
endif()

include_directories(SYSTEM ${BELLPORT_INCLUDE_DIR})

add_definitions(-DNOMINMAX)
add_definitions(-D__STDC_FORMAT_MACROS)
add_definitions(-DBP_BOOST_DATE_TIME_POSIX_TIME_STD_CONFIG)
add_definitions(-DBP_BOOST_MPL_LIMIT_LIST_SIZE=30)
add_definitions(-DBP_BOOST_MPL_CFG_NO_PREPROCESSED_HEADERS)
if(WIN32)
  add_definitions(-DWIN32_LEAN_AND_MEAN)
  #Prevent inclusion of winsock.h in windows.h
  add_definitions(-D_WINSOCKAPI_)
  #add_definitions(/DFD_SETSIZE=256)
  add_definitions(/D_CRT_SECURE_NO_WARNINGS)
  # Needed for VS2012 and VS2013
  add_definitions(-DTBB_USE_CAPTURED_EXCEPTION)
  add_definitions(/permissive-)
  add_definitions(/Zc:__cplusplus)
  add_compile_options("/std:c++17")
else(WIN32)
  add_definitions(-DLinux=1)
  SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra -std=c++17")
endif(WIN32)


if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 7.0)
  SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -faligned-new")
endif()

#-----------------------------------------------------------------------
# pthread
find_package(Threads)

#-----------------------------------------------------------------------
# OpenSSL
#find_package(OpenSSL)
#
#if(OPENSSL_FOUND)
#  set(BELLPORT_THIRDPARTY_LIBRARIES ${BELLPORT_THIRDPARTY_LIBRARIES} ${OPENSSL_LIBRARIES})
#endif()


# we can add individual examples like the following two lines
# but to be generic we use the GLOB
#add_executable(cme_aggregated cme_aggregated.cpp)
#target_link_libraries(cme_aggregated ${BELLPORT_LIBRARIES} ${BELLPORT_THIRDPARTY_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})


file (GLOB CPPS "*.cpp")
foreach (FILENAME ${CPPS})
  string(LENGTH ${FILENAME} FN_LENGTH)
  string(LENGTH ${CMAKE_CURRENT_SOURCE_DIR} CD_LENGTH)
  math(EXPR CD_LENGTH "${CD_LENGTH} + 1")
  math(EXPR SS_LENGTH "${FN_LENGTH} - 4 - ${CD_LENGTH}")
  string(SUBSTRING ${FILENAME} ${CD_LENGTH} ${SS_LENGTH} TARGET)

  message(STATUS "Generating ${TARGET}")
  add_executable(${TARGET} ${FILENAME})
  if(WIN32)
    target_link_libraries(${TARGET} ${BELLPORT_LIBRARIES} ${BELLPORT_THIRDPARTY_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
    if (NOT MSVC_VERSION LESS 1900)
      target_link_libraries(${TARGET} legacy_stdio_definitions)
    endif()
  else(WIN32)
    target_link_libraries(${TARGET} ${BELLPORT_LIBRARIES} ${BELLPORT_THIRDPARTY_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} rt)
  endif(WIN32)
endforeach()

if(WIN32)
  file (GLOB BINS "${BELLPORT_ROOT_DIR}/bin/*")
  foreach (FILENAME ${BINS})
    file (COPY ${FILENAME} DESTINATION "Debug/")
    file (COPY ${FILENAME} DESTINATION "Release/")
  endforeach()
endif()
