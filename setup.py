import sys

try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup

requirements = [req.strip() for req in open('requirements.pip')]

setup(
  name = 'leaderboard',
  version = "3.6.1",
  author = 'David Czarnecki',
  author_email = "dczarnecki@agoragames.com",
  packages = ['leaderboard'],
  install_requires = requirements,
  url = 'https://github.com/agoragames/leaderboard-python',
  license = "LICENSE.txt",
  description = 'Leaderboards backed by Redis in Python',
  long_description = open('README.md').read(),
  keywords = ['python', 'redis', 'leaderboard'],
  classifiers = [
    'Development Status :: 5 - Production/Stable',
    'License :: OSI Approved :: MIT License',
    "Intended Audience :: Developers",
    "Operating System :: POSIX",
    "Topic :: Communications",
    "Topic :: System :: Distributed Computing",
    "Topic :: Software Development :: Libraries :: Python Modules",
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Topic :: Software Development :: Libraries'
  ]
)
