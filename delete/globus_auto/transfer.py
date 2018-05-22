#!/usr/bin/env python

import config
import globus_sdk
import shelve
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

def transfer():
    """Sync the source and destination directories or send an error e-mail."""
    # Error checking.
    if config.CODE_PATH[len(config.CODE_PATH) - 1] != "/":
        print("Please add a trailing slash to CODE_PATH in config.py.")
        return
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
        # Add a mapping from file paths to task IDs if it does not already exist.
        if config.SHELF_FILE_TASK_IDS not in shelf.keys():
            shelf[config.SHELF_FILE_TASK_IDS] = {}
        # Retrieve the mapping from file paths to task IDs.
        file_task_ids = shelf[config.SHELF_FILE_TASK_IDS]
        # Add a list of task IDs for ongoing transfers if it does not already exist.
        if config.SHELF_TASK_ID_LIST not in shelf.keys():
            shelf[config.SHELF_TASK_ID_LIST] = []
        # Retrieve the list of task IDs for ongoing transfers.
        task_id_list = shelf[config.SHELF_TASK_ID_LIST]
        # Retrieve a list of all files in the directory and in all its sub-directories.
        file_paths = utils.globus_list_files(tc, config.SRC_ID, config.SRC_DIR)
        # Perform the transfer.
        task_id = utils.globus_sync_directory(tc, utils.globus_transfer_name(0), 
                                              config.SRC_ID, config.DST_ID, 
                                              config.SRC_DIR, config.DST_DIR)["task_id"]
        # Store the transfer's task id for each of the files in the transfer.
        for file_path in file_paths:
            file_task_ids[file_path] = task_id
        task_id_list.append(task_id)
        # Update and close the shelf.
        shelf[config.SHELF_TASK_ID_LIST] = task_id_list
        shelf[config.SHELF_FILE_TASK_IDS] = file_task_ids
        shelf.close()

if __name__ == "__main__":
    transfer()
