#!/usr/bin/env python
from distutils.core import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()
with open('README.md') as f:
    readme = f.read()

setup(name="blobuploader",
      description="Command-line client for uploading blobs to the Mozilla [blobber] server.",
      long_description=readme,
      version="1.2.2",
      author="Mihai Tabara",
      author_email="mtabara@mozilla.com",
      url="https://github.com/catlee/blobber",
      scripts=["blobberc.py"],
      license="MPL",
      install_requires=required,
      packages=['blobuploader'],
      package_dir={'blobuploader': 'blobuploader'},
      package_data={'blobuploader': ['*.pem']},
      include_package_data=True,
      )
