# Generated with autoscan, then modified appropriately.
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT
AC_CONFIG_SRCDIR([process.cpp])

#AC_ARG_WITH([boost-headers],
#  AC_HELP_STRING([--with-boost-headers=DIR],
#                 [find Boost headers in DIR]),
#	         [case "$with_boost_headers" in
#		    yes) AC_MSG_ERROR([
#*** --with-boost-headers requires an argument]) ;;
#		    ''|no) AC_MSG_ERROR([
#*** The Boost headers are REQUIRED and cannot
#*** be ommited using --without-boost-headers.]) ;;
#		    *) CPPFLAGS="$CPPFLAGS -I$withval"
#                       CFLAGS="$CFLAGS -I$withval"
#                       CXXFLAGS="$CXXFLAGS -I$withval" ;;
#	          esac],
#	         [])

# Checks for programs.
AC_PROG_CXX([g++])
AC_PROG_CC([gcc])

# Look for BOOST_PP.
#AC_LANG_PUSH([C++])
#AC_CHECK_HEADERS([boost/preprocessor/library.hpp], [], [AC_MSG_FAILURE([
#*** The BOOST_PP macros are missing or cannot be found.
#*** Did you try using --with-boost-headers=DIR?])], [])
#AC_LANG_POP([C++])

# Check for BOOST_FOREACH.
#AC_LANG_PUSH([C++])
#AC_CHECK_HEADERS([boost/foreach.hpp], [], [AC_MSG_FAILURE([
#*** The BOOST_FOREACH macro is missing or cannot be found.
#*** Did you try using --with-boost-headers=DIR?])], [])
#AC_LANG_POP([C++])

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h limits.h netdb.h netinet/in.h stdlib.h string.h sys/ioctl.h sys/socket.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_SIZE_T
AC_HEADER_TIME

# Checks for library functions.
AC_PROG_GCC_TRADITIONAL
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_FUNC_SELECT_ARGTYPES
AC_CHECK_FUNCS([gethostbyname gethostname getpagesize memset select socket])

AC_CONFIG_FILES([Makefile])
AC_CONFIG_SUBDIRS([third_party/libev-3.8])

AC_OUTPUT
