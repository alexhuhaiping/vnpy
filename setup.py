# encoding: UTF-8

'''
vn.py - By Traders, For Traders.

The vn.py project is an open-source quantitative trading framework
that is developed by traders, for traders.

The project is mainly written in Python and uses C++ for low-layer
and performance sensitive infrastructure.

Using the vn.py project, institutional investors and professional
traders, such as hedge funds, prop trading firms and investment banks,
can easily develop complex trading strategies with the Event Engine
Strategy Module, and automatically route their orders to the most
desired destinations, including equity, commodity, forex and many
other financial markets.
'''

import os
import platform
from setuptools import setup, find_packages, Extension

import vnpy


def getSubpackages(name):
    """获取该模块下所有的子模块名称"""
    splist = []

    for dirpath, _dirnames, _filenames in os.walk(name):
        if os.path.isfile(os.path.join(dirpath, '__init__.py')):
            splist.append(".".join(dirpath.split(os.sep)))

    return splist

with open('requirements.txt', 'r') as f:
    requirements = [r for r in f.readlines() if '#' not in r and r != '']

if platform.uname().system == "Windows":
    compiler_flags = ["/MP", "/std:c++17",  # standard
                      "/O2", "/Ob2", "/Oi", "/Ot", "/Oy", "/GL",  # Optimization
                      "/wd4819"  # 936 code page
                      ]
    extra_link_args = []
else:
    compiler_flags = ["-std=c++17",
                      "-Wno-delete-incomplete", "-Wno-sign-compare",
                      ]
    extra_link_args = ["-lstdc++"]

vnctpmd = Extension("vnpy.api.ctp.vnctpmd",
                    [
                        "vnpy/api/ctp/vnctp/vnctpmd/vnctpmd.cpp",
                    ],
                    include_dirs=["vnpy/api/ctp/include",
                                  "vnpy/api/ctp/vnctp", ],
                    define_macros=[],
                    undef_macros=[],
                    library_dirs=["vnpy/api/ctp/libs", "vnpy/api/ctp"],
                    libraries=["thostmduserapi", "thosttraderapi", ],
                    extra_compile_args=compiler_flags,
                    extra_link_args=extra_link_args,
                    depends=[],
                    runtime_library_dirs=["$ORIGIN"],
                    language="cpp",
                    )
vnctptd = Extension("vnpy.api.ctp.vnctptd",
                    [
                        "vnpy/api/ctp/vnctp/vnctptd/vnctptd.cpp",
                    ],
                    include_dirs=["vnpy/api/ctp/include",
                                  "vnpy/api/ctp/vnctp", ],
                    define_macros=[],
                    undef_macros=[],
                    library_dirs=["vnpy/api/ctp/libs", "vnpy/api/ctp"],
                    libraries=["thostmduserapi", "thosttraderapi", ],
                    extra_compile_args=compiler_flags,
                    extra_link_args=extra_link_args,
                    runtime_library_dirs=["$ORIGIN"],
                    depends=[],
                    language="cpp",
                    )

ext_modules = [vnctptd, vnctpmd]

setup(
    name="vnpy",
    version=vnpy.__version__,
    author="vn.py team",
    author_email="vn.py@foxmail.com",
    license="MIT",
    url="https://www.vnpy.com",
    description="A framework for developing quant trading systems.",
    long_description=__doc__,
    keywords='quant quantitative investment trading algotrading',
    include_package_data=True,
    packages=find_packages(),
    package_data={"": [
        "*.ico",
        "*.ini",
        "*.dll",
        "*.so",
        "*.pyd",
    ]},
    install_requires=requirements,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Operating System :: Microsoft :: Windows :: Windows 7",
        "Operating System :: Microsoft :: Windows :: Windows 8",
        "Operating System :: Microsoft :: Windows :: Windows 10",
        "Operating System :: Microsoft :: Windows :: Windows Server 2008",
        "Operating System :: Microsoft :: Windows :: Windows Server 2012",
        "Operating System :: Microsoft :: Windows :: Windows Server 2012",
        "Operating System :: POSIX :: Linux"
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Office/Business :: Financial :: Investment",
        "Programming Language :: Python :: Implementation :: CPython",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: Chinese (Simplified)",
        "Natural Language :: Chinese (Simplified)"
    ],
    # ext_modules=ext_modules
)