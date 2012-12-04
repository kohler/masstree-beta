dnl Process this file with autoconf to produce a configure script.

AC_INIT([masstree-beta], [0.1])
AC_PREREQ(2.60)
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([GNUmakefile])
AC_SUBST(ac_configure_args)

ac_user_cxx=${CXX+y}

AC_PROG_CC
AC_PROG_CXX
AC_LANG_CPLUSPLUS

AC_C_BIGENDIAN([AC_DEFINE([ENDIAN_BIG], [1], [big endian])],
               [AC_DEFINE([ENDIAN_LITTLE], [1], [little endian])],
               [AC_MSG_ERROR([Unknown endian])],
               [AC_MSG_ERROR([Universal endian])])

AC_CHECK_HEADERS([sys/epoll.h numa.h])

AC_SEARCH_LIBS([numa_available], [numa], [AC_DEFINE([HAVE_LIBNUMA], [1], [Define if you have libnuma.])])


dnl Builtins

AC_DEFUN([KVDB_CHECK_BUILTIN], [
    AC_CACHE_CHECK([for $1 builtin], [ac_cv_have_$1],
	[AC_LINK_IFELSE([AC_LANG_PROGRAM([$2], [$3])],
	    [ac_cv_have_$1=yes], [ac_cv_have_$1=no])])
    if test $ac_cv_have_$1 = yes; then
	AC_DEFINE(AS_TR_CPP([HAVE_$1]), [1], [Define if you have the $1 builtin.])
    fi
])

KVDB_CHECK_BUILTIN([__builtin_clz], [[unsigned x = 11;]],
    [[unsigned y = __builtin_clz(x);]])

KVDB_CHECK_BUILTIN([__builtin_clzl], [[unsigned long x = 11;]],
    [[unsigned long y = __builtin_clzl(x);]])

KVDB_CHECK_BUILTIN([__builtin_clzll], [[unsigned long long x = 11;]],
    [[unsigned long long y = __builtin_clzll(x);]])

KVDB_CHECK_BUILTIN([__builtin_ctz], [[unsigned x = 11;]],
    [[unsigned y = __builtin_ctz(x);]])

KVDB_CHECK_BUILTIN([__builtin_ctzl], [[unsigned long x = 11;]],
    [[unsigned long y = __builtin_ctzl(x);]])

KVDB_CHECK_BUILTIN([__builtin_ctzll], [[unsigned long long x = 11;]],
    [[unsigned long long y = __builtin_ctzll(x);]])

KVDB_CHECK_BUILTIN([__sync_synchronize], [[long x = 11;]],
    [[long *y = &x; __sync_synchronize();]])

KVDB_CHECK_BUILTIN([__sync_fetch_and_add], [[long x = 11;]],
    [[long y = __sync_fetch_and_add(&x, 2L);]])

KVDB_CHECK_BUILTIN([__sync_fetch_and_add_8], [[#include <stdint.h>
int64_t x = 11;]],
    [[int64_t y = __sync_fetch_and_add(&x, (int64_t) 2);]])

KVDB_CHECK_BUILTIN([__sync_bool_compare_and_swap], [[long x = 11, y = 12;]],
    [[__sync_bool_compare_and_swap(&x, y, 13L);]])

KVDB_CHECK_BUILTIN([__sync_bool_compare_and_swap_8], [[#include <stdint.h>
int64_t x = 11, y = 12;]],
    [[__sync_bool_compare_and_swap(&x, y, (int64_t) 13);]])

KVDB_CHECK_BUILTIN([__sync_val_compare_and_swap], [[long x = 11, y = 12;]],
    [[__sync_val_compare_and_swap(&x, y, 13L);]])

KVDB_CHECK_BUILTIN([__sync_val_compare_and_swap_8], [[#include <stdint.h>
int64_t x = 11, y = 12;]],
    [[__sync_val_compare_and_swap(&x, y, (int64_t) 13);]])

KVDB_CHECK_BUILTIN([__sync_lock_test_and_set], [[long x = 0;]],
    [[long y = __sync_lock_test_and_set(&x, 1);]])

KVDB_CHECK_BUILTIN([__sync_lock_test_and_set_val], [[long x = 0, y = 2;]],
    [[long z = __sync_lock_test_and_set(&x, y);]])

KVDB_CHECK_BUILTIN([__sync_lock_release], [[long x = 1;]],
    [[__sync_lock_release(&x);]])


dnl C++ features

AC_CACHE_CHECK([whether the C++ compiler understands 'auto'], [ac_cv_cxx_auto], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[struct s { int a; }; int f(s x) { auto &y = x; return y.a; }]], [[]])],
	[ac_cv_cxx_auto=yes], [ac_cv_cxx_auto=no])])
if test "$ac_cv_cxx_auto" != yes -a -z "$ac_user_cxx"; then
    CXX="${CXX} -std=gnu++0x"
    AC_MSG_CHECKING([whether the C++ compiler with -std=gnu++0x understands 'auto'])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[struct s { int a; }; int f(s x) { auto &y = x; return y.a; }]], [[]])],
	[ac_cv_cxx_auto=yes], [ac_cv_cxx_auto=no])
    AC_MSG_RESULT([$ac_cv_cxx_auto])
fi

if test "$ac_cv_cxx_auto" = yes; then
    AC_DEFINE([HAVE_CXX_AUTO], [1], [Define if the C++ compiler understands 'auto'.])
else
    AC_MSG_ERROR([

The C++ compiler does not appear to understand C++11.
To fix this problem, try supplying a "CXX" argument to ./configure,
such as "./configure CXX='c++ -std=gnu++0x'".

========================================================])
fi

AC_CACHE_CHECK([whether the C++ compiler understands constexpr], [ac_cv_cxx_constexpr], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[constexpr int f(int x) { return x + 1; }]], [[]])],
	[ac_cv_cxx_constexpr=yes], [ac_cv_cxx_constexpr=no])])
if test "$ac_cv_cxx_constexpr" = yes; then
    AC_DEFINE([HAVE_CXX_CONSTEXPR], [1], [Define if the C++ compiler understands constexpr.])
fi

AC_CACHE_CHECK([whether the C++ compiler understands static_assert], [ac_cv_cxx_static_assert], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[const int f = 2;]], [[static_assert(f == 2, "f should be 2");]])],
	[ac_cv_cxx_static_assert=yes], [ac_cv_cxx_static_assert=no])])
if test "$ac_cv_cxx_static_assert" = yes; then
    AC_DEFINE([HAVE_CXX_STATIC_ASSERT], [1], [Define if the C++ compiler understands static_assert.])
fi

AC_CACHE_CHECK([whether the C++ compiler understands rvalue references], [ac_cv_cxx_rvalue_references], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[int f(int &) { return 1; } int f(int &&) { return 0; }]], [[return f(int());]])],
	[ac_cv_cxx_rvalue_references=yes], [ac_cv_cxx_rvalue_references=no])])
if test "$ac_cv_cxx_rvalue_references" = yes; then
    AC_DEFINE([HAVE_CXX_RVALUE_REFERENCES], [1], [Define if the C++ compiler understands rvalue references.])
fi

AC_CACHE_CHECK([whether the C++ compiler understands template alias], [ac_cv_cxx_template_alias], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[template <typename T> struct X { typedef T type; }; template <typename T> using Y = X<T>; int f(int x) { return x; }]], [[return f(Y<int>::type());]])],
	[ac_cv_cxx_template_alias=yes], [ac_cv_cxx_template_alias=no])])
if test "$ac_cv_cxx_template_alias" = yes; then
    AC_DEFINE([HAVE_CXX_TEMPLATE_ALIAS], [1], [Define if the C++ compiler understands template alias.])
fi

AC_CHECK_HEADERS([type_traits])

AC_CACHE_CHECK([for __has_trivial_copy], [ac_cv_have___has_trivial_copy], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([], [[long x = 1; if (__has_trivial_copy(long)) x = 0;]])], [ac_cv_have___has_trivial_copy=yes], [ac_cv_have___has_trivial_copy=no])])
if test $ac_cv_have___has_trivial_copy = yes; then
    AC_DEFINE([HAVE___HAS_TRIVIAL_COPY], [1], [Define if you have the __has_trivial_copy compiler intrinsic.])
fi

if test "$ac_cv_cxx_rvalue_references" = yes; then
    AC_MSG_CHECKING([for std::move])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([#include <utility>], [[long x = 0; long &&y = std::move(x);]])], [ac_cv_std_move=yes], [ac_cv_std_move=no])
    AC_MSG_RESULT([$ac_cv_std_move])
    if test "$ac_cv_std_move" != yes; then
        AC_MSG_ERROR([

The C++ compiler understands C++11, but does not have std::move.
If you are using clang on Mac, ensure the -stdlib=libc++ option.

========================================================])
    fi
fi

AC_CACHE_CHECK([for std::is_trivially_copyable], [ac_cv_have_std_is_trivially_copyable], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([#include <type_traits>], [[return std::is_trivially_copyable<int>::value;]])], [ac_cv_have_std_is_trivially_copyable=yes], [ac_cv_have_std_is_trivially_copyable=no])])
if test $ac_cv_have_std_is_trivially_copyable = yes; then
    AC_DEFINE([HAVE_STD_IS_TRIVIALLY_COPYABLE], [1], [Define if you have the std::is_trivially_copyable template.])
fi

AC_CACHE_CHECK([for std::is_rvalue_reference], [ac_cv_have_std_is_rvalue_reference], [
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([#include <type_traits>], [[return std::is_rvalue_reference<int>::value;]])], [ac_cv_have_std_is_rvalue_reference=yes], [ac_cv_have_std_is_rvalue_reference=no])])
if test $ac_cv_have_std_is_rvalue_reference = yes; then
    AC_DEFINE([HAVE_STD_IS_RVALUE_REFERENCE], [1], [Define if you have the std::is_rvalue_reference template.])
fi


dnl Memory allocator

AC_CHECK_LIB([flow], [malloc], [have_flow=true], [have_flow=])
AC_CHECK_LIB([jemalloc], [malloc], [have_jemalloc=true], [have_jemalloc=])
AC_CHECK_LIB([tcmalloc_minimal], [malloc], [have_tcmalloc_minimal=true], [have_tcmalloc_minimal=])

AC_ARG_WITH([malloc],
    [AS_HELP_STRING([--with-malloc=TYPE],
                    [Memory allocator (malloc|jemalloc|tcmalloc|flow)])],
    [ac_kvdb_malloc=$withval], [ac_kvdb_malloc=yes])

if test \( "$ac_kvdb_malloc" = tcmalloc -a -z "$have_tcmalloc_minimal" \) \
	-o \( "$ac_kvdb_malloc" = jemalloc -a -z "$have_jemalloc" \) \
	-o \( "$ac_kvdb_malloc" = flow -a -z "$have_flow" \); then
    AC_MSG_ERROR([$ac_kvdb_malloc not found])
elif test "$ac_kvdb_malloc" = yes -o "$ac_kvdb_malloc" = default; then
    AC_MSG_CHECKING([for malloc library])
    if test -n "$have_flow"; then ac_kvdb_malloc=flow;
    elif test -n "$have_jemalloc"; then ac_kvdb_malloc=jemalloc;
    elif test -n "$have_tcmalloc_minimal"; then ac_kvdb_malloc=tcmalloc;
    else ac_kvdb_malloc=malloc; fi
    AC_MSG_RESULT([$ac_kvdb_malloc])
elif test "$ac_kvdb_malloc" = no -o "$ac_kvdb_malloc" = malloc -o -z "$ac_kvdb_malloc"; then
    ac_kvdb_malloc=malloc
else
    AC_MSG_ERROR([Unknown malloc type $ac_kvdb_malloc])
fi

if test "$ac_kvdb_malloc" = tcmalloc; then
    MALLOC_LIBS="-ltcmalloc_minimal"
    AC_DEFINE([HAVE_TCMALLOC], [1], [Define if you are using libtcmalloc for malloc.])
elif test "$ac_kvdb_malloc" = jemalloc; then
    MALLOC_LIBS="-ljemalloc"
    AC_DEFINE([HAVE_JEMALLOC], [1], [Define if you are using libjemalloc for malloc.])
elif test "$ac_kvdb_malloc" = flow; then
    MALLOC_LIBS="-lflow"
    AC_DEFINE([HAVE_FLOW_MALLOC], [1], [Define if you are using libflow for malloc.])
else
    MALLOC_LIBS=
fi
AC_SUBST(MALLOC_LIBS)


dnl Types

AC_DEFUN([KVDB_CHECK_SAME_TYPE], [
    pushdef([KVDB_CST_VAR], [AS_TR_SH([ac_cv_have_same_type_$1_is_$2])])
    AC_CACHE_CHECK([whether $1 and $2 are the same type], KVDB_CST_VAR,
	[AC_COMPILE_IFELSE([AC_LANG_PROGRAM([$3
int f($1) {return 0;} int f($2) {return 0;}], [])],
	    [KVDB_CST_VAR=no], [KVDB_CST_VAR=yes])])
    if test $KVDB_CST_VAR = yes; then
	AC_DEFINE(AS_TR_CPP([HAVE_$1_IS_$2]), [1], [Define if $1 and $2 are the same type.])
    fi
    popdef([KVDB_CST_VAR])
])

KVDB_CHECK_SAME_TYPE([off_t], [long], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([off_t], [long long], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([int64_t], [long], [#include <stdint.h>])
KVDB_CHECK_SAME_TYPE([int64_t], [long long], [#include <stdint.h>])
KVDB_CHECK_SAME_TYPE([size_t], [unsigned], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([size_t], [unsigned long], [#include <stdio.h>])
KVDB_CHECK_SAME_TYPE([size_t], [unsigned long long], [#include <stdio.h>])

AC_CHECK_TYPES([long long])
AC_CHECK_SIZEOF([int])
AC_CHECK_SIZEOF([long])
AC_CHECK_SIZEOF([long long])
AC_CHECK_SIZEOF([void *])

AC_CHECK_DECLS([getline])

AC_CHECK_HEADERS([time.h])
AC_CHECK_DECLS([clock_gettime], [], [], [#if HAVE_TIME_H
# include <time.h>
#endif])
AC_SEARCH_LIBS([clock_gettime], [rt])
AC_CHECK_FUNCS([clock_gettime])


AC_ARG_ENABLE([row-type],
    [AS_HELP_STRING([--enable-row-type=ARG],
                    [Row type: bag array array_ver str, default bag])],
    [ac_cv_row_type=$enableval], [ac_cv_row_type=bag])
if test "$ac_cv_row_type" = array; then
    AC_DEFINE_UNQUOTED([KVDB_ROW_TYPE_ARRAY], [1], [Define if the default row type is kvdb_timed_array.])
elif test "$ac_cv_row_type" = array_ver; then
    AC_DEFINE_UNQUOTED([KVDB_ROW_TYPE_ARRAY_VER], [1], [Define if the default row type is kvdb_timed_array_ver.])
elif test "$ac_cv_row_type" = bag; then
    AC_DEFINE_UNQUOTED([KVDB_ROW_TYPE_BAG], [1], [Define if the default row type is kvdb_timed_bag.])
elif test "$ac_cv_row_type" = str; then
    AC_DEFINE_UNQUOTED([KVDB_ROW_TYPE_STR], [1], [Define if the default row type is kvdb_timed_str.])
else
    AC_MSG_ERROR([$ac_cv_row_type: Unknown kvdb row type])
fi

AC_ARG_ENABLE([max-key-len],
    [AS_HELP_STRING([--enable-max-key-len=ARG],
                    [Maximum length of a key in bytes, default 255])],
    [ac_cv_max_key_len=$enableval], [ac_cv_max_key_len=255])
AC_DEFINE_UNQUOTED([KVDB_MAX_KEY_LEN], [$ac_cv_max_key_len], [Maximum key length])

AC_ARG_ENABLE([max-row-len],
    [AS_HELP_STRING([--enable-max-row-len=ARG],
                    [Maximum length of a row in bytes, default 65535])],
    [ac_cv_max_row_len=$enableval], [ac_cv_max_row_len=65535])
AC_DEFINE_UNQUOTED([KVDB_MAX_ROW_LEN], [$ac_cv_max_row_len], [Maximum row length])

AC_ARG_ENABLE([superpage],
    [AS_HELP_STRING([--disable-superpage],
	    [Disable superpage support])],
    [ac_cv_superpage=$enableval], [ac_cv_superpage=yes])
if test "$ac_cv_superpage" != no; then
    AC_DEFINE_UNQUOTED([HAVE_SUPERPAGE_ENABLED], [1], [Define if superpage support is enabled.])
fi

AC_ARG_ENABLE([memdebug],
    [AS_HELP_STRING([--enable-memdebug],
	    [Enable memory debugging])])
if test "$enable_memdebug" = yes; then
    AC_DEFINE_UNQUOTED([HAVE_MEMDEBUG], [1], [Define if memory debugging support is enabled.])
fi

AC_ARG_ENABLE([assert],
    [AS_HELP_STRING([--disable-assert],
	    [Disable debugging assertions])])
if test "$enable_assert" = no; then
    AC_DEFINE_UNQUOTED([KVDB_NDEBUG], [1], [Define to disable debugging assertions.])
fi

AH_BOTTOM([#if !KVDB_DEBUG && KVDB_NDEBUG
# define NDEBUG 1
#endif])

AC_DEFINE_UNQUOTED([HAVE_UNALIGNED_ACCESS], [1], [Define if unaligned accesses are OK.])

AC_OUTPUT
