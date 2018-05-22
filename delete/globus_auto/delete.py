#!/usr/bin/env python

import config
import datetime
import globus_sdk
import shelve
import time
import utils

"""This code deletes successfully transferred files from the source directory, and can be 
automated via a CRON job.

It checks transfers generated by main.py and deletes files and state in the storage shelf relating 
to successful transfers.

All settings can be found in config.py.

"""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

def delete():
    """Delete files from the source directory that have been successfully transferred."""
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
        email_text = "Automatic delete is not possible due to the following errors:\n"
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
        # Remove all no longer existent files from the mapping from file paths to task IDs.
        file_paths = utils.globus_list_files(tc, config.SRC_ID, config.SRC_DIR)
        delete = []
        for file_path in file_task_ids.keys():
            if file_path not in file_paths:
                delete.append(file_path)
        for file_path in delete:
            del file_task_ids[file_path]
        # Stage successfully transferred files for deletion.
        delete = []
        successful_task_ids = []
        for file_path in file_task_ids.keys():
            task_id = file_task_ids[file_path]
            if task_id in successful_task_ids:
                delete.append(file_path)
            elif tc.get_task(task_id)["status"] == "SUCCEEDED":
                successful_task_ids.append(task_id)
                delete.append(file_path)
        # Perform the delete and remove files and tasks from mappings.
        delete_name = utils.globus_transfer_name(1)
        delete_task_id = None
        if delete:
            delete_task_id = utils.globus_delete_paths(tc, delete_name,
                                                       config.SRC_ID, delete)["task_id"]
            for file_path in delete:
                del file_task_ids[file_path]
            active_task_ids = []
            for task_id in task_id_list:
                task = tc.get_task(task_id)
                if task["status"] == "ACTIVE":
                    active_task_ids.append(task_id)
            task_id_list = active_task_ids
        # Update and close the shelf.
        shelf[config.SHELF_TASK_ID_LIST] = task_id_list
        shelf[config.SHELF_FILE_TASK_IDS] = file_task_ids
        shelf.close()
        # Wait at most 3 minutes for files to be deleted before checking for empty directories.
        if delete_task_id:
            starting_time = datetime.datetime.now()
            while (datetime.datetime.now() - starting_time).total_seconds() / 60 < 3:
                delete_status = tc.get_task(delete_task_id)["status"]
                if delete_status == "SUCCEEDED" or delete_status == "FAILED":
                    break
                time.sleep(10)
        # Delete empty directories.
        utils.globus_delete_empty_directories(tc, delete_name, config.SRC_ID, config.SRC_DIR)

if __name__ == "__main__":
    delete()