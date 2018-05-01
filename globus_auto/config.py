#!/usr/bin/env python

# TRANSFER DETAILS ################################################################################

# The absolute path to the source directory in the source endpoint.
SRC_DIR = ""
# The absolute path to the target directory in the destination endpoint.
DST_DIR = ""

# PREFERENCES #####################################################################################

# The e-mail address from which error e-mails are sent.
EMAIL_SENDER = ""
# The recipients of error e-mails in a comma-separated list.
EMAIL_RECIPIENTS = []

# SETTINGS ########################################################################################

# The ID of the source GCP endpoint.
SRC_ID = ""
# The ID of the destination endpoint.
DST_ID = ""
# The ID of the client application performing Globus transfers.
CLIENT_ID = ""
# The absolute path to the directory in which main.py resides, ending with a trailing slash.
CODE_PATH = "/globus_auto/"
# The absolute path to the installation of Python3 in the source endpoint.
PYTHON_PATH = "python"

# MISSION CRITICAL [DO NOT EDIT] ##################################################################

# The host over which e-mails are sent.
EMAIL_HOST = "smtp.lbl.gov"
# The subject line of error e-mails.
EMAIL_SUBJECT = "Globus Automatic Transfer Errors"
# The absolute path to the refresh token in the source endpoint.
TOKEN_PATH = CODE_PATH + "refresh_token"
# The name of the persistent shelf.
SHELF_NAME = "datastore"
# The absolute path to the persistent shelf.
SHELF_PATH = CODE_PATH + SHELF_NAME
