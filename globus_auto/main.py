#!/usr/bin/env python

import config
import globus_sdk
import os
import shelve
import sys
import utils

"""This code performs a single Globus transfer, and can be automated via a CRON job.

It syncs the contents of a destination directory in an endpoint with the contents of a source 
directory in another endpoint. If either endpoint in a transfer is not ready, an e-mail is sent to 
the administrator until errors are resolved.

Transfers are encrypted and verified using checksums.

All settings are found in config.py.

"""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

def main():
    """Check for changes and transfer to the appropriate endpoint or send an error e-mail."""
    # Initialize a transfer client given the application ID and refresh token path.
    tc = utils.globus_get_transfer_client(config.CLIENT_ID, config.TOKEN_PATH)
    # Determine whether or not the source and destination endpoints are ready for transfer.
    src_ready = utils.globus_endpoint_ready(tc, config.SRC_ID)
    dst_ready = utils.globus_endpoint_ready(tc, config.DST_ID)
    endpoints_ready = src_ready and dst_ready
    if not endpoints_ready:
        # Determine the text of the error e-mail based on errors.
        email_text = "Automatic transfer is not possible due to the following errors:\n"
        if not src_ready:
            email_text += "Endpoint " + config.SRC_ID + " is not ready.\n"
        if not dst_ready:
            email_text += "Endpoint " + config.DST_ID + " is not ready.\n"
        # Send an e-mail reporting the relevant errors.
        try:
            utils.send_email(config.EMAIL_HOST, config.EMAIL_SUBJECT, config.EMAIL_SENDER,
                             config.EMAIL_RECIPIENTS, email_text)
        except:
            print("Globus: There was an error in sending an error e-mail.")
    else:
        # Create a new or open an existing shelf, used for persisting state.
        shelf = shelve.open(config.SHELF_PATH)
        # Only perform transfers if files were added or changed, determined by comparing a hash.
        need_transfer = False
        for dirpath, dirnames, filenames in os.walk(config.SRC_DIR):
            for filename in filenames:
                abs_path = str(os.path.join(dirpath, filename))
                md5_hash = utils.md5_hash(abs_path)
                if abs_path not in shelf.keys() or md5_hash != shelf[abs_path]:
                    shelf[abs_path] = md5_hash
                    need_transfer = True
        if need_transfer:
            # Perform the transfer.
            utils.globus_sync_directory(tc, utils.globus_transfer_name(), config.SRC_ID,
                                        config.DST_ID, config.SRC_DIR, config.DST_DIR)
        shelf.close()

if __name__ == "__main__":
    main()
