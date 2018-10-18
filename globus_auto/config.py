#!/usr/bin/env python

import os

# TRANSFER DETAILS ################################################################################

# The absolute path to the source directory in the source endpoint.
SRC_DIR = ""
# The absolute path to the destination directory in the destination endpoint.
DST_DIR = ""

# SETTINGS ########################################################################################

# The ID of the source endpoint.
SRC_ID = ""
# The ID of the destination endpoint.
DST_ID = ""
# The ID of the client application performing Globus transfers.
CLIENT_ID = ""
# The absolute path to the directory in which main.py resides.
CODE_PATH = ""

# MISSION CRITICAL [DO NOT EDIT] ##################################################################

# The format for dates.
DATE_FORMAT = "%Y-%m-%d_%H%M%S"
# The absolute path to the log.
LOG_PATH = os.path.join(CODE_PATH, "log")
# The name of the persistent shelf.
SHELF_NAME = "datastore"
# The absolute path to the persistent shelf.
SHELF_PATH = os.path.join(CODE_PATH, SHELF_NAME)
# The key to the timestamp denoting the last transfer in the shelf.
SHELF_TIMESTAMP_KEY = "GLOBAL_TIMESTAMP"
# The key to the trie in the shelf.
SHELF_TRIE_KEY = "TRIE"
# The absolute path to the refresh token in the source endpoint.
TOKEN_PATH = os.path.join(CODE_PATH, "refresh_token")
