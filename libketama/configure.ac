#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.59])
AC_INIT([libketama], [0.1], [bclark@wayfair.com])
AM_INIT_AUTOMAKE([foreign])
AC_CONFIG_SRCDIR([ketama.c])
AC_CONFIG_HEADERS(config.h)
# Checks for programs.
AC_PROG_AWK
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_LN_S

# Checks for libraries.
# FIXME: Replace `main' with a function in `-lm':
# AC_CHECK_LIB([m], [main])

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h syslog.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MKTIME
AC_FUNC_REALLOC
AC_CHECK_FUNCS([strtol])
AC_CONFIG_FILES([Makefile])
AC_OUTPUT
echo "
    Prefix.......: $prefix
"
