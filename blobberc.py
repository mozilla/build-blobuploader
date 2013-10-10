#!/usr/bin/env python
"""Usage: blobberc.py -u URL... -a AUTH_FILE -b BRANCH [-v] [-d] FILE

-u, --url URL          URL to blobber server to upload to.
-a, --auth AUTH_FILE   user/pass AUTH_FILE for signing the calls
-b, --branch BRANCH    Specify branch for the file (e.g. try, mozilla-central)
-v, --verbose          Increase verbosity
-d, --dir              Instead of a file, upload multiple files from a dir name

FILE                   Local file(s) to upload
"""
import urlparse
import os
import hashlib
import requests
import logging
import random
from functools import partial

from blobuploader import cert

log = logging.getLogger(__name__)


def filehash(filename, hashalgo):
    h = hashlib.new(hashalgo)
    with open(filename, 'rb') as f:
        for block in iter(partial(f.read, 1024 ** 2), ''):
            h.update(block)
    return h.hexdigest()


def upload_file(hosts, filename, branch, auth, hashalgo='sha512',
                blobhash=None, attempts=10):

    if blobhash is None:
        blobhash = filehash(filename, hashalgo)

    log.info("Uploading %s ...", filename)
    host_pool = hosts[:]
    random.shuffle(host_pool)
    non_retryable_codes = (202, 401, 403)
    n = 1

    while n <= attempts and host_pool:
        host = host_pool.pop()
        log.info("Using %s", host)
        log.info("Uploading, attempt #%d.", n)
        # TODO: _post_file() may barf in open() and exit, add a work around

        ret_code = _post_file(host, auth, filename, branch, hashalgo, blobhash)
        if ret_code in non_retryable_codes:
            log.info("Blobserver returned %s, bailing...", ret_code)
            break

        log.info("Upload failed. Trying again ...")
        n += 1

    log.info("Done attempting.")


def _analyze_response(response, default_filename):
    filename = response.headers.get('x-blob-filename',
                                    default_filename)
    ret_code = response.status_code

    if ret_code == 202:
        blob_url = response.headers.get('x-blob-url')
        if not blob_url:
            log.critical("Blob storage URL not found in response!")
            return
        ret = requests.head(blob_url)
        if ret.ok:
            log.info("TinderboxPrint: Uploaded %s to %s", filename,
                     blob_url)
        else:
            log.warning("File uploaded to blobserver but failed uploading "
                        "to Amazon S3.")
    else:
        err_msg = response.headers.get('x-blobber-msg',
                                       'Something went wrong on blobber!')
        log.critical(err_msg)


def _post_file(host, auth, filename, branch, hashalgo, blobhash):
    url = urlparse.urljoin(host, '/blobs/{}/{}'.format(hashalgo, blobhash))

    data_dict = dict(blob=open(filename, "rb"))
    meta_dict = dict(branch=branch)

    log.debug("Uploading file to %s ...", url)
    response = requests.post(url, auth=auth, files=data_dict, data=meta_dict,
                             verify=cert.where())

    _analyze_response(response, filename)
    return response.status_code


def upload_dir(hosts, dirname, branch, auth):
    log.info("Open directory for files ...")
    # Ignore directories and symlinks
    files = [f for f in os.listdir(dirname) if
             os.path.isfile(os.path.join(dirname, f)) and
             not os.path.islink(os.path.join(dirname, f))]

    log.debug("Go through all files in directory")
    for f in files:
        filename = os.path.join(dirname, f)
        upload_file(hosts, filename, branch, auth)

    log.info("Iteration through files over.")


def main():
    from docopt import docopt

    args = docopt(__doc__)

    if args['--verbose']:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    FORMAT = "(blobuploader) - %(levelname)s - %(message)s"
    logging.basicConfig(format=FORMAT, level=loglevel)
    logging.getLogger('requests').setLevel(logging.WARN)

    credentials = {}
    execfile(args['--auth'], credentials)
    auth = (credentials['blobber_username'], credentials['blobber_password'])

    if args['--dir']:
        upload_dir(args['--url'], args['FILE'], args['--branch'], auth)
    else:
        upload_file(args['--url'], args['FILE'], args['--branch'], auth)


if __name__ == '__main__':
    main()
