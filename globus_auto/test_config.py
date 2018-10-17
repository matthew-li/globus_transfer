#!/usr/bin/env python

import config
import globus_sdk
import os
import unittest
import utils

"""This code tests that the configuration is valid, that a refresh token exists, and that 
endpoints are ready. The user is prompted to generate a token if one does not exist."""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

def main():
    test_case = unittest.TestCase()
    # Test that local paths exist, are absolute, and point to directories.
    for path in [config.SRC_DIR, config.CODE_PATH]:
        if not os.path.isabs(path):
            test_case.fail("{} is not an absolute path.".format(path))
        if not utils.dir_exists(path):
            test_case.fail("{} is not an existing directory.".format(path))
    tc = utils.globus_get_transfer_client(config.CLIENT_ID, config.TOKEN_PATH)
    # Test that the token exists.
    if not utils.file_exists(config.TOKEN_PATH):
        test_case.fail("The token does not exist.")
    # Test that endpoints are ready.
    for endpoint_id in [config.SRC_ID, config.DST_ID]:
        if not utils.globus_endpoint_ready(tc, endpoint_id):
            test_case.fail("Endpoint {} is not ready.")
    print("The configuration is valid, the refresh token exists, and endpoints are ready.")

if __name__ == "__main__":
    main()
