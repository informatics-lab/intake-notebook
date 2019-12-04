#!/usr/bin/env python
#-----------------------------------------------------------------------------
# Copyright (c) 2012 - 2018, Anaconda, Inc. and Intake contributors
# All rights reserved.
#
# The full license is in the LICENSE file, distributed with this software.
#-----------------------------------------------------------------------------

from setuptools import setup, find_packages
import versioneer

INSTALL_REQUIRES = ['intake >=0.5.2']

setup(
    name='intake-notebook',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Notebook plugins for Intake',
    url='https://github.com/NickMortimer/intake-notebook.git',
    maintainer='Nick Mortimer',
    maintainer_email='nick.mortimer@csiro.au',
    license='BSD',
    py_modules=['intake_notebook'],
    packages=find_packages(),
    entry_points={
        'intake.drivers': [
            'notebook = intake_notebook.notebook_source:NotebookSource',
            'experiment = intake_notebook.experiment_source:ExperimentSource',
        ]
    },
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    zip_safe=False, )
