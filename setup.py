#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='flowy',
    version='0.2.1',
    description="Python library for Amazon Simple Workflow Service",
    long_description=open('README.rst').read(),
    keywords='amazon swf simple workflow',
    author='Sever Banesiu',
    author_email='banesiu.sever@gmail.com',
    url='https://github.com/pbs/flowy',
    license='MIT License',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    setup_requires=['nose>=1.0'],
    install_requires=['boto==2.19.0', 'venusian>=1.0a8'],
    tests_require=['mock', 'coverage'],
    test_suite="nose.collector",
    entry_points={
        "console_scripts": [
            "flowy = flowy.__main__:main"
        ]
    }
)
