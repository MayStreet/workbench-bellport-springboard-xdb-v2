#-- cmake/modules/FindSolarCapture.cmake ------------------------------*- CMake -*-==#
#
#                 Bellport Low Latency Trading Infrastructure.
#
# Copyright MayStreet 2014 - all rights reserved
#
# $Id: 235f42cf90bfb1ae92e21026bf4fb61a34f09271 $
#===----------------------------------------------------------------------====#
# - Try to find SolarCapture Library
# Once done this will define
#
#  SOLARCAPTURE_LIBRARY_FOUND - system has OpenOnload
#  SOLARCAPTURE_INCLUDE_DIR - the OpenOnload include directory
#  SOLARCAPTURE_LIBRARIES - Link these to use OpenOnload

FIND_PATH(SOLARCAPTURE_INCLUDE_DIR
  NAMES solar_capture.h
  PATHS ${BELLPORT_THIRDPARTY_ROOT}/solar_capture/
  )

FIND_LIBRARY(SOLARCAPTURE_LIBRARY
  solarcapture1
  )

set(SOLARCAPTURE_LIBRARIES debug ${SOLARCAPTURE_LIBRARY} optimized ${SOLARCAPTURE_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set SOLARCAPTURE_FOUND to TRUE if
# all listed variables are TRUE
FIND_PACKAGE_HANDLE_STANDARD_ARGS(SolarCapture DEFAULT_MSG SOLARCAPTURE_LIBRARY SOLARCAPTURE_INCLUDE_DIR)

MARK_AS_ADVANCED(SOLARCAPTURE_INCLUDE_DIR SOLARCAPTURE_LIBRARIES)

