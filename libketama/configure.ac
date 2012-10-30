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
AC_CANONICAL_HOST
case "${host}" in
  *-*-freebsd*)
	CFLAGS="-pipe -fno-strict-aliasing"
	LDFLAGS="$LDFLAGS -shared -lm -Wl,-soname,libketama.so.1"
	;;
  *-*-linux*)
	CFLAGS="$CFLAGS"
	LDFLAGS+=" -shared -lm -Wl,-soname,libketama.so.1"
	;;
  *-*-darwin*)
	CFLAGS+=" -fPIC -W -Wall -Werror -isysroot /Developer/SDKs/MacOSX10.5.sdk -arch ppc -arch i386 -arch ppc64 -arch x86_64"
	LDFLAGS+=" -g -dynamiclib -isysroot /Developer/SDKs/MacOSX10.5.sdk -arch ppc -arch i386 -arch ppc64 -arch x86_64"
	;;
  *-*-solaris2*)
	CFLAGS="$(CFLAGS) -DSOLARIS"
	LDFLAGS="$(LDFLAGS) -shared -lm"
	;;
  *)
	AC_MSG_RESULT([Unsupported operating system: ${host}, assuming defaults known to work on linux and freebsd])
	CFLAGS="$CFLAGS"
	LDFLAGS+="$LDFLAGS -shared -lm -Wl,-soname,libketama.so.1"
	;;
esac

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
