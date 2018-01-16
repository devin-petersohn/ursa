from setuptools import find_packages, setup
from version import version as ursa_version

setup(
    name='ursa',
    version=ursa_version,
    description='Graph on top of ray',
    author='Devin Petersohn',
    author_email='devin.petersohn@berkeley.edu',
    url="https://github.com/devin-petersohn/ursa",
    install_requires=[],
    packages=find_packages(exclude=['*.test.*']))
