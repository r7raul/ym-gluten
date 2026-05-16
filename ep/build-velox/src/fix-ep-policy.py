import sys
f = 'cpp/cmake_modules/ThirdpartyToolchain.cmake'
content = open(f).read()

# Fix 1: add CMAKE_POLICY_VERSION_MINIMUM=3.5 to EP_COMMON_CMAKE_ARGS
old1 = '    -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE})'
new1 = '    -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE}\n    -DCMAKE_POLICY_VERSION_MINIMUM=3.5)'
if old1 in content:
    content = content.replace(old1, new1, 1)
    print(f"Patched {f}: added CMAKE_POLICY_VERSION_MINIMUM=3.5 to EP_COMMON_CMAKE_ARGS")
else:
    print(f"WARNING: EP_COMMON_CMAKE_ARGS pattern not found, may already be patched")

# Fix 2: skip BOOST_PROCESS_NEED_SOURCE for Boost >= 1.86 (already fixed upstream)
old2 = '''      target_compile_definitions(Boost::process INTERFACE "BOOST_PROCESS_HAVE_V2")
      # Boost < 1.86 has a bug that
      # boost::process::v2::process_environment::on_setup() isn\'t
      # defined. We need to build Boost Process source to define it.
      #
      # See also:
      # https://github.com/boostorg/process/issues/312
      target_compile_definitions(Boost::process INTERFACE "BOOST_PROCESS_NEED_SOURCE")'''
new2 = '''      target_compile_definitions(Boost::process INTERFACE "BOOST_PROCESS_HAVE_V2")
      if(Boost_VERSION VERSION_LESS 1.86)
        # Boost < 1.86 has a bug that
        # boost::process::v2::process_environment::on_setup() isn\'t
        # defined. We need to build Boost Process source to define it.
        #
        # See also:
        # https://github.com/boostorg/process/issues/312
        target_compile_definitions(Boost::process INTERFACE "BOOST_PROCESS_NEED_SOURCE")
      endif()'''
if old2 in content:
    content = content.replace(old2, new2, 1)
    print(f"Patched {f}: BOOST_PROCESS_NEED_SOURCE skipped for Boost >= 1.86")
else:
    print(f"WARNING: BOOST_PROCESS_NEED_SOURCE pattern not found, may already be patched")

open(f, 'w').write(content)

# Fix 3: replace BOOST_PROCESS_V2_ASIO_NAMESPACE (not defined in Boost >= 1.86 integrated)
# with boost::asio which is the correct namespace in Boost-integrated builds.
f3 = 'cpp/src/arrow/testing/process.cc'
content3 = open(f3).read()
old3 = 'namespace asio = BOOST_PROCESS_V2_ASIO_NAMESPACE;'
new3 = 'namespace asio = boost::asio;'
if old3 in content3:
    content3 = content3.replace(old3, new3, 1)
    open(f3, 'w').write(content3)
    print(f"Patched {f3}: replaced BOOST_PROCESS_V2_ASIO_NAMESPACE with boost::asio")
else:
    print(f"INFO: {f3} already patched or pattern not found")
