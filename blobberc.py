#!/usr/bin/env python
"""Usage: blobberc.py -u URL... -b BRANCH [-v] [-d] FILE

-u, --url URL          URL to blobber server to upload to.
-b, --branch BRANCH    Specify branch for the file (e.g. try, mozilla-central)
-v, --verbose          Increase verbosity
-d, --dir              Instead of a file, upload multiple files from a dir name

FILE                   Local file(s) to upload
"""
import urlparse
import os
import urllib2
import hashlib
import requests
import poster.encode
import logging
import random
from functools import partial


log = logging.getLogger(__name__)


def filehash(filename, hashalgo):
    h = hashlib.new(hashalgo)
    with open(filename, 'rb') as f:
        for block in iter(partial(f.read, 1024 ** 2), ''):
            h.update(block)
    return h.hexdigest()


sha512sum = partial(filehash, hashalgo='sha512')
s3_bucket_base_url = 'http://mozilla-releng-blobs.s3.amazonaws.com/blobs'


def upload_file(hosts, filename, branch, hashalgo='sha512',
                blobhash=None, attempts=2):

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
            'filename': filename,
            'branch': branch,
            'hashalgo': hashalgo,
            'blobhash': blobhash,
        }
        log.info("POST call the file - attempt #%d." % (n))
        if _post_file(**post_params):
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
        else:
            log.warning("POST call failed. Trying again ...")

        n += 1

    if n == attempts+1:
        log.warning("Nr. of attempts exceeded. Uploading %s file failed!" %
                 (filename))


def _post_file(host, filename, branch, hashalgo, blobhash):
    url = urlparse.urljoin(host, '/blobs/{}/{}'.format(hashalgo, blobhash))

    datagen, headers = poster.encode.multipart_encode({
        'data': open(filename, 'rb'),
        'filename': filename,
        'filesize': os.path.getsize(filename),
        'branch': branch,
        'mimetype': 'application/octet-stream',
    })
    req = urllib2.Request(url, datagen, headers)
    log.debug("Posting file to %s ...", url)
    try:
        urllib2.urlopen(req)
    except urllib2.URLError:
        log.debug("Posting file %s failed." % filename)
        return False

    log.debug("Posting file %s sucessfully." % filename)
    return True


def upload_dir(hosts, dirname, branch, hashalgo='sha512'):
    log.info("Open directory for files ...")
    dir_files = [f for f in os.listdir(dirname)
                 if os.path.isfile(os.path.join(dirname, f))]

    log.debug("Go through all files in directory")
    for f in dir_files:
        filename = os.path.join(dirname, f)
        upload_file(hosts, filename, branch)

    log.info("Iteration through files over.")


def main():
    from docopt import docopt
    import poster.streaminghttp
    poster.streaminghttp.register_openers()

    args = docopt(__doc__)

    if args['--verbose']:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    FORMAT = "(blobuploader) - %(levelname)s - %(message)s"
    logging.basicConfig(format=FORMAT, level=loglevel)
    logging.getLogger('requests').setLevel(logging.WARN)

    if args['--dir']:
        if os.path.isdir(args['FILE']):
            upload_dir(args['--url'], args['FILE'], branch=args['--branch'])
    else:
        if os.path.isfile(args['FILE']):
            upload_file(args['--url'], args['FILE'], branch=args['--branch'])

if __name__ == '__main__':
    main()
