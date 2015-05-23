#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup
import sys

# with open('README.rst') as file:
# 	long_description = file.read()

# extras = {}
# if sys.version_info < (3, 3):
# 	extras['tests_require'] = ['mock']

setup(
	name = 'vycody',
	version = '0.1',
	description = 'Distributed redis job - queue system',
	long_description = long_description,
	author = 'Paul Skopnik',
	author_email = 'paul@skopnik.me',
	license = 'MIT',
	url = 'http://gl.wo.skopnik.pw/pskopnik/vycody',
	provides = ['vycody'],
	packages = ['vycody', 'vycody.modules'],
	install_requires = [
		"argh>=0.25"
	],
	entry_points = {
		'console_scripts': [
			'vycody=vycody.cli:main'
		]
	},
	# extras_require = {
	# 	'mongodb': ['pymongo'],
	# 	'mysql': ['pymysql']
	# },
	# test_suite = "gitdh.tests",
	# keywords = "git deploy deployment commit database remote approval cron post-receive hook",
	# classifiers = [
	# 	'Development Status :: 4 - Beta',
	# 	'License :: OSI Approved :: MIT License',
	# 	'Operating System :: POSIX :: Linux',
	# 	'Programming Language :: Python :: 3',
	# 	'Programming Language :: Python :: 3.2',
	# 	'Programming Language :: Python :: 3.3',
	# 	'Programming Language :: Python :: 3.4',
	# 	'Topic :: Software Development :: Version Control',
	# ],
	**extras
)
