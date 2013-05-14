AC_DEFUN([AC_CHECK_EXTRA_OPTIONS],[
        AC_MSG_CHECKING(for debugging)
        AC_ARG_ENABLE(debug, [  --enable-debug  compile for debugging])
        if test -z "$enable_debug" ; then
                enable_debug="no"
        elif test $enable_debug = "yes" ; then
                CPPFLAGS="${CPPFLAGS} -g -D_DEBUG"
        fi
        AC_MSG_RESULT([$enable_debug])
])

