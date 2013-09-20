#!/usr/bin/env python
from setuptools import setup

setup(name="blobuploader",
      version="0.9",
      author="Mihai Tabara",
      author_email="mtabara@mozilla.com",
      url="https://github.com/catlee/blobber",
      scripts = ["blobberc.py"],
      install_requires=["poster==0.8.1",
                        "requests==1.2.3.",
                        "docopt==0.6.1"],
      description="Specific client for uploading blob files on Mozilla server",
      )
