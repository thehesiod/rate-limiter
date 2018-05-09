#!/usr/bin/env python
from setuptools import setup

_packages = {
    'rate_limiter': 'rate_limiter',
}


def my_test_suite():
    import asynctest
    test_loader = asynctest.TestLoader()
    test_suite = test_loader.discover('tests')
    return test_suite


setup(
    name="rate-limiter",
    version='0.1.1',
    description='Rate Limiter',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    author='Alexander Mohr',
    author_email='thehesiod@gmail.com',
    package_dir=_packages,
    packages=list(_packages.keys()),
    install_requires=[
    ],
    test_suite='setup.my_test_suite',
)
