import subprocess
from setuptools import setup, find_packages, Extension

setup(
  name='Cassandra FDW',
  version='1.0.1',
  author='Rankactive',
  link = 'https://rankactive.com/',
  license='Postgresql',
  packages=['cassandra-fdw']
)