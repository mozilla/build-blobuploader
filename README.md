blobuploader is a command-line client for uploading to the [blobber] server.

Installation
============

Run `pip install -r requirements.txt` to install Python prerequisites.

Run `python setup.py install` to install the modules and command-line tools.

Configuration
=============

You will need to provide an authentication file containing a username and password matching those you specified in your blobber server config.

The file contents should look like:

````
blobber_username = 'username'
blobber_password = 'password'
````

Usage
=====

Run `blobberc.py -u *URL* -a *auth file* -b *branch name* *path to file*` to upload a file to the blobber server at *URL*.

*auth file* should be the path to the file containing the username/password you've previously configured. *branch name* is a tag to indicate which branch this blob belongs to. Note that blobber has a hardcoded [whitelist] of file types that are acceptable, as well as a [whitelist] of allowed IP addresses.

[blobber]: https://github.com/catlee/blobber
[whitelist]: https://github.com/catlee/blobber/blob/master/blobber/config.py#L6
