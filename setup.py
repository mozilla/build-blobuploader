#!/usr/bin/env python
from distutils.core import setup

setup(name="blobuploader",
      version="0.993",
      author="Mihai Tabara",
      author_email="mtabara@mozilla.com",
      url="https://github.com/catlee/blobber",
      scripts = ["blobberc.py"],
      license="MPL",
      install_requires=["requests==1.2.3.",
                        "docopt==0.6.1"],
      description="Specific client for uploading blob files on Mozilla server",
      packages=['blobuploader'],
      package_dir={'blobuploader': 'blobuploader'},
      package_data={'blobuploader': ['*.pem']},
      include_package_data=True,
      )
