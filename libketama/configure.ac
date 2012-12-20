AC_INIT(libketama, version-0.1)

AC_ARG_WITH(fnv, [  --with-fnv            enable FNV-1a hash support])

AC_CHECK_HEADER(ketama.h)
AC_CHECK_HEADER(stdio.h)
AC_CHECK_HEADER(string.h)
AC_CHECK_HEADER(errno.h)
AC_CHECK_HEADER(time.h)
AC_CHECK_HEADER(unistd.h)
AC_CHECK_HEADER(math.h)
AC_CHECK_HEADER(sys/stat.h)
AC_CHECK_HEADER(sys/shm.h)
AC_CHECK_HEADER(sys/sem.h)

AC_CHECK_HEADER(fnv.h)
AC_CHECK_HEADER(stdlib.h)

AC_CHECK_HEADER(md5.h)

AC_DEFINE(POINTS_PER_SERVER, 160)
if test "$with_fnv" == "yes"
then
    AC_MSG_NOTICE([Using FNV-1a hash algorithm])
    AC_DEFINE(ENABLE_FNV_HASH, 1)
    AC_DEFINE(POINTS_PER_HASH, 1)
    MODULE_HASH="fnv_32a"
else
    AC_MSG_NOTICE([Using MD5 hash algorithm])
    AC_DEFINE(POINTS_PER_HASH, 4)
    MODULE_HASH="md5"
fi
cp confdefs.h ketama_config.h

AC_SUBST(MODULE_HASH)
AC_OUTPUT(Makefile)
