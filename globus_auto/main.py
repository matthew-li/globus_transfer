#!/usr/bin/env python

import config
import shelve
import utils

"""This code performs a single Globus transfer, copying files and directories across endpoints 
whose timestamps are newer on the source side than on the destination side."""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

def main():
    """Check for changes and transfer to the appropriate endpoint if ready."""
    logger = utils.get_logger(__name__, config.LOG_PATH)
    logger.info("".join(["=" for i in range(100)]))
    logger.info("Checking if the directory was modified...")
    shelf = shelve.open(config.SHELF_PATH)
    if config.SHELF_TIMESTAMP_KEY not in shelf:
        shelf[config.SHELF_TIMESTAMP_KEY] = None
    global_timestamp = shelf[config.SHELF_TIMESTAMP_KEY]
    last_modified = utils.last_modified(config.SRC_DIR, config.DATE_FORMAT)
    directory_modified = not global_timestamp or global_timestamp < last_modified
    if not directory_modified:
        logger.info("The directory was not modified, so a transfer is not necessary.")
        return
    tc = utils.globus_get_transfer_client(config.CLIENT_ID, config.TOKEN_PATH)
    logger.info("Checking if endpoints are ready...")
    src_ready = utils.globus_endpoint_ready(tc, config.SRC_ID)
    dst_ready = utils.globus_endpoint_ready(tc, config.DST_ID)
    endpoints_ready = src_ready and dst_ready
    if not endpoints_ready:
        if not src_ready:
            logger.error("Endpoint {} is not ready.".format(config.SRC_ID))
        if not dst_ready:
            logger.error("Endpoint {} is not ready.".format(config.DST_ID))
    else:
        logger.info("Endpoints are ready.")
        if config.SHELF_TRIE_KEY not in shelf:
            shelf[config.SHELF_TRIE_KEY] = utils.GlobusDirectoryTrie(config.SRC_DIR)
        trie = shelf[config.SHELF_TRIE_KEY]
        logger.info("Scanning directory...")
        trie.add_new_paths()
        logger.info("Checking for additions or changes...")
        (dirs, files) = trie.get_transfer_paths()
        if not dirs and not files:
            logger.info("There were no additions or changes, so a transfer is not necessary.")
        dir_pairs = utils.get_src_dst_pairs(dirs, config.SRC_DIR, config.DST_DIR)
        if dir_pairs:
            logger.info("Attempting to create empty directories...")
            utils.globus_create_dirs(tc, config.DST_ID, [dst for (src, dst) in dir_pairs.items()])
            dir_source_paths = [src_path for src_path in dir_pairs]
            trie.set_transfer_times(dir_source_paths, utils.DirectoryObject.DIR)
        file_pairs = utils.get_src_dst_pairs(files, config.SRC_DIR, config.DST_DIR)
        if file_pairs:
            transfer_name = utils.globus_transfer_name(config.DATE_FORMAT)
            num_files = len(file_pairs)
            logger.info("Initiating transfer {} ({} file(s))...".format(transfer_name, num_files))
            try:
                task_id = utils.globus_transfer_files(tc, transfer_name, config.SRC_ID, 
                                                      config.DST_ID, file_pairs)["task_id"]
                logger.info("Submitted transfer {}.".format(task_id))
                file_source_paths = [src_path for src_path in file_pairs]
                trie.set_transfer_times(file_source_paths, utils.DirectoryObject.FILE)
            except Exception as e:
                logger.info("Failed to initiate transfer {}:\n{}.".format(transfer_name, e))
        global_timestamp = utils.datetime_now(config.DATE_FORMAT)
        shelf[config.SHELF_TRIE_KEY] = trie
    logger.info("Setting the global timestamp to {}.".format(global_timestamp))
    shelf[config.SHELF_TIMESTAMP_KEY] = global_timestamp
    logger.info("Saving changes.")
    shelf.close()

if __name__ == "__main__":
    main()
