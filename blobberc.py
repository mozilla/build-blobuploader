#!/usr/bin/env python
"""Usage: blobberc.py -u URL... -a AUTH_FILE -b BRANCH [-v] [-d] [-z] FILE

-u, --url URL          URL to blobber server to upload to.
-a, --auth AUTH_FILE   user/pass AUTH_FILE for signing the calls
-b, --branch BRANCH    Specify branch for the file (e.g. try, mozilla-central)
-v, --verbose          Increase verbosity
-d, --dir              Instead of a file, upload multiple files from a dir name
-z, --gzip             gzip compress file before uploading

FILE                   Local file(s) to upload
"""

import urlparse
import os
import hashlib
import requests
import logging
import random
import tempfile
import traceback
import gzip
from functools import partial

from blobuploader import cert

log = logging.getLogger(__name__)

# These filetypes get gzip-compressed by default
default_compress_filetypes = set(['.txt', '.log', '.html'])

def filehash(f, hashalgo):
    """
    Return hash for a given file object and hashing algorithm

    """
    h = hashlib.new(hashalgo)
    for block in iter(partial(f.read, 1024 ** 2), ''):
        h.update(block)
    return h.hexdigest()


def should_compress(filename):
    """
    Return True if the file should be compressed before uploading.
    """
    return os.path.splitext(filename)[1].lower() in default_compress_filetypes

def upload_dir(hosts, dirname, branch, auth, compress=False):
    """
    Sequentially call uploading subroutine for each file in the dir

    """
    log.info("Open directory for files ...")
    # Ignore directories and symlinks
    files = [f for f in os.listdir(dirname) if
             os.path.isfile(os.path.join(dirname, f)) and
             not os.path.islink(os.path.join(dirname, f))]

    log.debug("Go through all files in directory")
    for f in files:
        filename = os.path.join(dirname, f)
        compress_file = compress or should_compress(filename)
        upload_file(hosts, filename, branch, auth, compress=compress_file)

    log.info("Iteration through files over.")


def upload_file(hosts, filename, branch, auth, hashalgo='sha512',
                blobhash=None, attempts=10, compress=False):
    """
    Uploading subroutine is used to upload a single file to the first available
    host out of those specified. The hosts are randomly shuffled before
    sequentially attempting to make any calls. Should they return any of the
    following codes, results in breaking from the attempting gear to avoid
    duplicating the same call to other hosts too:
        #202 - file successfully uploaded to blobserver
        #401 - request requires user authentication
        #403 - bad credentials/IP forbidden/no file attached/
               missing metadata/file type forbidden/metadata limit exceeded

    """
    if compress:
        tf = tempfile.NamedTemporaryFile("wb")
        with gzip.GzipFile(filename, "wb", fileobj=tf) as gz:
            with open(filename, "rb") as f:
                gz.writelines(f)
        tf.flush()
        file = open(tf.name, "rb")
    else:
        file = open(filename, "rb")
    if blobhash is None:
        blobhash = filehash(file, hashalgo)
        file.seek(0)

    log.info("Uploading %s ...", filename)
    host_pool = hosts[:]
    random.shuffle(host_pool)
    non_retryable_codes = (401, 403)
    n = 1

    while n <= attempts and host_pool:
        host = host_pool.pop()
        log.info("Using %s", host)
        log.info("Uploading, attempt #%d.", n)

        try:
            ret = post_file(host, auth, file, filename, branch, hashalgo,
                            blobhash, compress)
        except:
            log.critical("Unexpected error in client: %s", traceback.format_exc())
            break

        if ret == 202:
            log.info("Blobserver returned %s. File uploaded!", ret)
            break

        if ret in non_retryable_codes:
            log.info("Blobserver returned %s, bailing...", ret)
            break

        log.info("Upload failed. Trying again ...")
        n += 1

    log.info("Done attempting.")


def check_status(response):
    """
    Read response from blob server and print accordingly log messages.
    If the file was successfully uploaded, a HEAD request is made
    to double check file availability on Amazon S3 storage

    """
    ret_code = response.status_code

    if ret_code == 202:
        blob_url = response.headers.get('x-blob-url')
        if not blob_url:
            log.critical("Blob storage URL not found in response!")
            return
        ret = requests.head(blob_url)
        if ret.ok:
            filename = response.headers.get('x-blob-filename')
            if not filename:
                log.critical("Blob filename not found in response!")
                return
            log.info("TinderboxPrint: <a href='%s'>%s</a>: uploaded", blob_url,
                     filename)
        else:
            log.warning("File uploaded to blobserver but failed uploading "
                        "to Amazon S3.")
    else:
        err_msg = response.headers.get('x-blobber-msg',
                                       'Something went wrong on blobber!')
        log.critical(err_msg)


def post_file(host, auth, file, filename, branch, hashalgo, blobhash,
              compressed):
    """
    Pack the request with all required information and make the call to host.
    Before returning response status code, it calls check_status to print
    accordingly log messages

    """
    url = urlparse.urljoin(host, '/blobs/{0}/{1}'.format(hashalgo, blobhash))
    data_dict = dict(blob=(os.path.basename(filename), file))
    meta_dict = dict(branch=branch)
    if compressed:
        meta_dict[compressed] = compressed

    log.debug("Uploading file to %s ...", url)
    # make the request call to blob server
    response = requests.post(url, auth=auth, files=data_dict, data=meta_dict,
                             verify=cert.where())

    check_status(response)
    return response.status_code


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
        upload_file(args['--url'], args['FILE'], args['--branch'], auth,
                    compress=args['--gzip'] or should_compress(args['FILE']))


if __name__ == '__main__':
    main()
