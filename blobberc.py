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


sha512sum = partial(filehash, hashalgo='sha512')
s3_bucket_base_url = 'http://mozilla-releng-blobs.s3.amazonaws.com/blobs'


def upload_file(hosts, filename, branch, auth, hashalgo='sha512',
                blobhash=None, attempts=10):

    if blobhash is None:
        blobhash = filehash(filename, hashalgo)

    log.info("Try to upload %s" % filename)
    host_pool = hosts[:]
    n = 1
    while n <= attempts:
        random.shuffle(host_pool)
        host = host_pool[0]
        log.info("Picked up %s host after shuffling." % host)
        post_params = {
            'host': host,
            'auth': auth,
            'filename': filename,
            'branch': branch,
            'hashalgo': hashalgo,
            'blobhash': blobhash,
        }
        log.info("POST call the file - attempt #%d." % (n))
        ret_code = _post_file(**post_params)
        if ret_code == 202:
            # File posted successfully via blob server.
            # Make sure the resource is available on amazon S3 bucket.
            resource_url = '%s/%s/%s/%s' % (s3_bucket_base_url, branch,
                                            hashalgo, blobhash)
            ret = requests.head(resource_url)
            if ret.ok:
                log.info("Uploaded %s to %s" % (filename, resource_url))
            else:
                log.warning("Uploading to Amazon S3 failed.")
            break
        elif ret_code == 403 or ret_code == 401:
            break
        else:
            log.warning("POST call failed. Trying again ...")

        n += 1

    if n == attempts+1:
        log.warning("Nr. of attempts exceeded. Uploading %s file failed!" %
                 (filename))


def _post_file(host, auth, filename, branch, hashalgo, blobhash):
    url = urlparse.urljoin(host, '/blobs/{}/{}'.format(hashalgo, blobhash))

    data_dict = {
        'blob': open(filename, "rb"),
    }

    meta_dict = {
        'branch': branch,
    }

    log.debug("Posting file to %s ...", url)
    req = requests.post(url,
                        auth=auth,
                        files=data_dict,
                        data=meta_dict,
                        verify=cert.where())

    if req.status_code != 202:
        err_msg = req.headers.get('x-blobber-msg',
                                  'Something went wrong on blobber!')
        log.warning(err_msg)

    return req.status_code


def upload_dir(hosts, dirname, branch, auth, hashalgo='sha512'):
    log.info("Open directory for files ...")
    dir_files = [f for f in os.listdir(dirname)
                 if os.path.isfile(os.path.join(dirname, f))]

    log.debug("Go through all files in directory")
    for f in dir_files:
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
        if os.path.isdir(args['FILE']):
            upload_dir(args['--url'],
                       args['FILE'],
                       args['--branch'],
                       auth)
    else:
        if os.path.isfile(args['FILE']):
            upload_file(args['--url'],
                        args['FILE'],
                        args['--branch'],
                        auth)

if __name__ == '__main__':
    main()
