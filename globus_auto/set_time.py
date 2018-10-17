#!/usr/bin/env python

import argparse
import config
import os
import shelve
import utils

"""This code allows the user to set the time at which the last transfer occurred, either to avoid 
transferring old files or to retransfer files."""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

def main():
    """Prompts the user to update the last transfer time in the shelf."""
    parser = argparse.ArgumentParser()
    help_message = ("Specify a path for which to set the time. If it points to a directory, the "
                    "times for all its contained entries will be set recursively.")
    parser.add_argument("--path", help=help_message)
    args = parser.parse_args()
    path = config.SRC_DIR
    try:
        if args.path:
            path = utils.validate_user_path(args.path)
            if not path.startswith(config.SRC_DIR):
                raise RuntimeError("The path {} is not in {}.".format(path, config.SRC_DIR))
        last_transfer_time = utils.get_user_datetime(config.DATE_FORMAT)
    except Exception as e:
        print(e)
        return
    logger = utils.get_logger(__name__, config.LOG_PATH)
    logger.info("".join(["=" for i in range(100)]))
    shelf = shelve.open(config.SHELF_PATH)
    if config.SHELF_TIMESTAMP_KEY not in shelf:
        shelf[config.SHELF_TIMESTAMP_KEY] = None
    global_timestamp = shelf[config.SHELF_TIMESTAMP_KEY]
    if not global_timestamp or last_transfer_time < global_timestamp:
        logger.info("Setting the global timestamp to {}.".format(last_transfer_time))
        shelf[config.SHELF_TIMESTAMP_KEY] = last_transfer_time
    if config.SHELF_TRIE_KEY not in shelf:
        shelf[config.SHELF_TRIE_KEY] = utils.GlobusDirectoryTrie(config.SRC_DIR)
    trie = shelf[config.SHELF_TRIE_KEY]
    logger.info("Setting individual timestamps in {} to {}.".format(path, last_transfer_time))
    trie.add_new_paths()
    (found, node_type, _) = trie.find(path)
    if found and utils.DirectoryObject.is_valid(node_type):
        trie.set_data_recursive(path, last_transfer_time)
    shelf[config.SHELF_TRIE_KEY] = trie
    logger.info("Saving changes.")
    shelf.close()

if __name__ == "__main__":
    main()
