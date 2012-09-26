"""Installer for skyhooks
"""

import os
cwd = os.path.dirname(__file__)
__version__ = open(os.path.join(cwd, 'skyhooks', 'version.txt'),
                    'r').read().strip()

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()

setup(
    name='skyhooks',
    description='Webhook handling utilities for asynchronous python apps ',
    version=__version__,
    author='Wes Mason',
    author_email='wes@serverdensity.com',
    url='https://github.com/serverdensity/skyhooks',
    packages=find_packages('src', exclude=['ez_setup']),
    install_requires=open(os.path.join(cwd, 'requirements.txt')).readlines(),
    package_data={'skyhooks': ['version.txt']},
    include_package_data=True
)
