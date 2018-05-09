#!/usr/bin/env python
from setuptools import setup
import os, re


_packages = {
    'rate_limiter': 'rate_limiter',
}


def my_test_suite():
    import asynctest
    test_loader = asynctest.TestLoader()
    test_suite = test_loader.discover('tests')
    return test_suite


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'rate_limiter', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in '
                               'rate_limiter/__init__.py')


setup(
    name="rate-limiter",
    version=read_version(),
    description='Rate Limiter',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    author='Alexander Mohr',
    author_email='thehesiod@gmail.com',
    url='https://github.com/thehesiod/rate_limiter',
    package_dir=_packages,
    packages=list(_packages.keys()),
    install_requires=[
    ],
    test_suite='setup.my_test_suite',
)
