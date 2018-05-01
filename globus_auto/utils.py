#!/usr/bin/env python

import globus_sdk
import hashlib
import os
import smtplib
from datetime import datetime, time
from email.mime.text import MIMEText

"""Utilities."""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

def globus_endpoint_ready(tc, endpoint_id):
    """Check that an endpoint is ready for transfer.

    Keyword Arguments:

    tc -- a transfer client, necessary to check requirements
    endpoint_id -- the ID of the endpoint
    """
    endpoint = tc.get_endpoint(endpoint_id)
    if endpoint["is_globus_connect"]:
        return endpoint["gcp_connected"] and not endpoint["gcp_paused"]
    else:
        reqs = tc.endpoint_get_activation_requirements(endpoint_id)
        return reqs["expires_in"] == -1 or reqs["activated"]

def globus_generate_refresh_token(auth_client, client_id, token_path):
    """Generate a refresh token for the given Globus Auth client having the given application ID 
       at the given path.

    Keyword Arguments:
    auth_client -- The AuthorizationClient to check
    client_id -- the ID of the client application
    token_path -- the path to the refresh token
    """
    # Define a generic input function.
    get_input = getattr(__builtins__, "raw_input", input)
    # Authorize using a code from the user.
    authorize_url = auth_client.oauth2_get_authorize_url()
    print("Please go to this URL and login: {0}".format(authorize_url))
    auth_code = get_input(
        "Please enter the code you get after login here: ").strip()
    token_response = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]
    # Write the refresh token to the given path, as well as return it.
    transfer_rt = globus_transfer_data["refresh_token"]
    transfer_config = open(token_path, "w")
    transfer_config.write(client_id + "\n")
    transfer_config.write(transfer_rt)
    transfer_config.close()
    return transfer_rt

def globus_get_transfer_client(client_id, token_path):
    """Generate and return a TransferClient by initializing an NativeAppAuthClient and a 
       RefreshTokenAuthorizer.

    Keyword Arguments:
    client_id -- the ID of the client application
    token_path -- the path to the refresh token
    """
    # Create and initiate an Auth client.
    auth_client = globus_sdk.NativeAppAuthClient(client_id)
    auth_client.oauth2_start_flow(refresh_tokens=True)
    # Retrieve a refresh token or generate one if it does not exist.
    if os.path.exists(token_path) and os.path.isfile(token_path):
        transfer_config = open(token_path, "r")
        line = transfer_config.readline().rstrip("\n")
        if line == client_id:
            transfer_rt = transfer_config.readline()
        else:
            transfer_rt = globus_generate_refresh_token(auth_client, client_id, token_path)
    else:
        transfer_rt = globus_generate_refresh_token(auth_client, client_id, token_path)
    # Create an authorizer for the refresh token.
    authorizer = globus_sdk.RefreshTokenAuthorizer(transfer_rt, auth_client)
    # Return a transfer client given the authorizer.
    return globus_sdk.TransferClient(authorizer=authorizer)

def globus_sync_directory(tc, transfer_name, src_id, dst_id, src_path, dst_path):
    """Transfer files from source to destination such that the latter has all the files of the 
    former and return the response.

    Keyword Arguments:
    tc -- a transfer client, necessary to perform a transfer
    transfer_name -- a name for the transfer
    src_id -- the ID of the source endpoint
    dst_id -- the ID of the destination endpoint
    src_path -- the path to the directory in the source
    dst_path -- the path to the directory in the destination
    """
    tdata = globus_sdk.TransferData(tc, src_id, dst_id, label=transfer_name,
                                    sync_level="checksum", encrypt_data=True)
    tdata.add_item(src_path, dst_path, recursive=True)
    return tc.submit_transfer(tdata)

def globus_transfer_name():
    """Return a name for a Globus transfer, consisting of the time."""
    return "AUTO_" + datetime.now().strftime("%Y-%m-%d_%H%M")

def md5_hash(path):
    """Return an md5 hash of the contents of the file at the given path.

    Keyword Arguments:
    path -- the path in which the file is located
    """
    m = hashlib.md5()
    m.update(path.encode("utf-8"))
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            m.update(chunk)
    return m.hexdigest()

def send_email(host, subject, sender, recipients, text):
    """Send an e-mail with the given parameters.

    Keyword Arguments:
    host -- the SMTP server to use
    subject -- the subject line of the e-mail
    sender -- the sending e-mail address
    recipients -- the receiving e-mail addresses
    text -- the text of the e-mail
    """
    msg = MIMEText(text)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    s = smtplib.SMTP(host)
    s.sendmail(sender, recipients, msg.as_string())
    s.quit()
