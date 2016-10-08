#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

install_requires = ['boto3==1.3.1',
                    'venusian>=1.0',
                    'lazy_object_proxy==1.2.2']
setup(name='flowy',
      version='0.4.1',
      description="A workflow modeling and execution library with gradual concurrency inference.",
      long_description=open('README.rst').read(),
      keywords='distributed workflow modeling SWF',
      author='Sever Banesiu',
      author_email='banesiu.sever@gmail.com',
      url='https://github.com/severb/flowy',
      license='MIT License',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      setup_requires=['setuptools_git', 'wheel'],
      install_requires=install_requires,
      extras_require={
          'docs': ['sphinx', 'sphinx_rtd_theme'],
          'trace': ['pygraphviz'],
          ':python_version == "2.7"': ['futures'],
      },
      entry_points={
          "console_scripts": ["flowy = flowy.__main__:main"]
      },
      classifiers=["Development Status :: 5 - Production/Stable",
                   "Intended Audience :: Developers",
                   "License :: OSI Approved :: MIT License",
                   "Operating System :: OS Independent",
                   "Topic :: Internet",
                   "Programming Language :: Python :: 2.7",
                   "Programming Language :: Python :: 3.4",
                   "Programming Language :: Python :: 3.5"], )
