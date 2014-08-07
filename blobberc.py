#!/home/armenzg/repos/tmp/build/venv/bin/python
"""Usage: blobberc.py -u URL... -a AUTH_FILE -b BRANCH [-o URL_FILE] [-v] [-d] [-z] FILE

-u, --url URL                         URL to blobber server to upload to.
-a, --auth AUTH_FILE                  user/pass AUTH_FILE for signing the calls
-b, --branch BRANCH                   Specify branch for the file (e.g. try, mozilla-central)
-o, --output-manifest-url URL_FILE    Track all uploaded files, upload the info to blobber
                                      and write the URL to such uploaded file.
-v, --verbose                         Increase verbosity
-d, --dir                             Instead of a file, upload multiple files from a dir name
-z, --gzip                            gzip compress file before uploading

FILE                                  Local file(s) to upload
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
import json
from functools import partial

from blobuploader import cert

log = logging.getLogger(__name__)

# These filetypes get gzip-compressed by default
default_compress_filetypes = set(['.txt', '.log', '.html'])
default_allowed_types = set(['zip', 'dmp', 'txt', 'log', 'jpg', 'png'])

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


def allowed_to_send(filename, whitelist):
    """
    Return True if the file can be send to the server, by checking its filetype
    in the server whitelist
    """
    return os.path.splitext(filename)[1].lower()[1:] in whitelist


def get_server_whitelist(hosts):
    """
    Return the extension whitelist from server within an API call.
    """
    hostname = hosts[0]
    url = urlparse.urljoin(hostname, '/blobs/whitelist')
    response = requests.get(url, verify=cert.where())
    return set(response.json().get('whitelist', []))


def upload_dir(hosts, dirname, branch, auth, compress=False,
               filetype_whitelist=default_allowed_types,
               manifest_url_file=None):
    """
    Sequentially call uploading subroutine for each file in the dir

    If manifest_url_file is specified, we track all files uploaded in a file which
    we upload to blobber and we store the URL in manifest_url_file.

    """
    log.info("Open directory for files ...")
    # Ignore directories and symlinks
    files = [f for f in os.listdir(dirname) if
             os.path.isfile(os.path.join(dirname, f)) and
             not os.path.islink(os.path.join(dirname, f))]

    upload_manifest = {}
    log.debug("Go through all files in directory")
    for f in files:
        filename = os.path.join(dirname, f)
        compress_file = compress or should_compress(filename)
        upload_manifest[f] = upload_file(hosts, filename, branch, auth, compress=compress_file,
                                         allowed=allowed_to_send(filename, filetype_whitelist))

    if upload_manifest and manifest_url_file:
        log.info("Uploading to blobber a json structured file with all "
                 "the files that have been uploaded.")
        log.info("Here are the contents: %s" % json.dumps(upload_manifest))
        # 1) Write to disk before we upload
        uploaded_files_filepath = 'uploaded_files.json'
        with open(uploaded_files_filepath, 'w') as outfile:
            json.dump(upload_manifest, outfile)
        # 2) Upload the file
        blob_url = upload_file(hosts, uploaded_files_filepath, branch, auth,
                compress=compress_file, allowed=allowed_to_send(filename, filetype_whitelist))
        if blob_url:
            # 3) Record URL of the uploaded summary file
            with open(manifest_url_file, 'w') as f:
                f.write(blob_url)
        else:
            log.warning("The summary file could not be uploaded")

    log.info("Iteration through files over.")


def upload_file(hosts, filename, branch, auth, hashalgo='sha512',
                blobhash=None, attempts=10, compress=False, allowed=False):
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

    It also calls check_status to print accordingly log messages
    It also returns the URL of the file that is uploaded
    """
    log.info("Uploading %s ...", filename)

    if not allowed:
        log.info("Skipped %s. File type not allowed on server.", filename)
        return

    if compress:
        file = tempfile.NamedTemporaryFile("w+b")
        with gzip.GzipFile(filename, "wb", fileobj=file) as gz:
            with open(filename, "rb") as f:
                gz.writelines(f)
        file.flush()
        file.seek(0)
    else:
        file = open(filename, "rb")
    if blobhash is None:
        blobhash = filehash(file, hashalgo)
        file.seek(0)

    blob_url = None
    host_pool = hosts[:]
    random.shuffle(host_pool)
    non_retryable_codes = (401, 403)
    n = 1

    while n <= attempts and host_pool:
        host = host_pool.pop()
        log.info("Using %s", host)
        log.info("Uploading, attempt #%d.", n)

        try:
            response = post_file(host, auth, file, filename, branch, hashalgo,
                                 blobhash, compress)
            check_status(response)
            ret = response.status_code
            blob_url = response.headers.get('x-blob-url')
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
    return blob_url


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

    Returns an HTTP response.

    """
    url = urlparse.urljoin(host, '/blobs/{0}/{1}'.format(hashalgo, blobhash))
    data_dict = dict(blob=(os.path.basename(filename), file))
    meta_dict = dict(branch=branch)
    if compressed:
        meta_dict['compressed'] = compressed

    log.debug("Uploading file to %s ...", url)
    # make the request call to blob server
    return requests.post(url, auth=auth, files=data_dict, data=meta_dict,
                         verify=cert.where())

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

    filetype_whitelist = get_server_whitelist(args['--url'])
    if not filetype_whitelist:
        filetype_whitelist = default_allowed_types

    if args['--dir']:
        manifest_url_file = args['--output-manifest-url'] \
                if args.has_key('--output-manifest-url') else None
        upload_dir(args['--url'], args['FILE'], args['--branch'], auth,
                   filetype_whitelist=filetype_whitelist,
                   manifest_url_file=manifest_url_file)
    else:
        upload_file(args['--url'], args['FILE'], args['--branch'], auth,
                    compress=args['--gzip'] or should_compress(args['FILE']),
                    allowed=allowed_to_send(args['FILE'], filetype_whitelist))


if __name__ == '__main__':
    main()
