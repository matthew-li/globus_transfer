#!/usr/bin/env python

import globus_sdk
import os
import smtplib
from datetime import datetime, time
from email.mime.text import MIMEText

"""Utilities."""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

class Directory(object):
    """A structure representing a directory.

    It contains the absolute path to the directory, a list of Directory objects for its 
    subdirectories, and a list of its contained files.
    """
    def __init__(self, path):
        """Instantiate a Directory object.

        Keyword Arguments:
        self -- the class object
        """
        # The absolute path to the directory being represented.
        self.path = path
        # A list of directory objects representing the directory's subdirectories.
        self.directories = []
        # A list of absolute paths to the directory's immediate files.
        self.files = []
        # Whether or not the directory or any of its subdirectories contains files.
        self.contains_files = False

    def set_contains_files(self):
        """For the current directory, and each of its subdirectories, set and return whether or 
        not the directory, or any of its subdirectories, contains files.

        Keyword Arguments:
        self -- the class object
        path -- the absolute path to the top level directory to be converted to a Directory object
        """
        if self.files:
            self.contains_files = True
        for entry in self.directories:
            self.contains_files = entry.set_contains_files() or self.contains_files
        return self.contains_files

def globus_delete_empty_directories(tc, delete_name, endpoint_id, top_dir):
    """Delete empty directories in the given top-level directory, but not that directory itself, 
    at the given endpoint and return the response.

    Keyword Arguments:
    tc -- a transfer client, necessary to perform a delete
    delete_name -- a name for the delete
    endpoint_id -- the ID of the endpoint at which the directory is located
    top_dir -- the top level directory to delete from
    """
    def list_fileless_directories(directory):
        """Return a list of the absolute paths to the shallowest directories such that neither 
        they nor their subdirectories contain files.

        Keyword Arguments:
        directory -- an object of the Directory class
        """
        directories = []
        for entry in directory.directories:
            if entry.contains_files:
                directories.extend(list_fileless_directories(entry))
            else:
                directories.append(entry.path)
        return directories
    top_directory = globus_get_file_structure(tc, endpoint_id, top_dir)
    top_directory.set_contains_files()
    deletable_paths = list_fileless_directories(top_directory)
    return globus_delete_paths(tc, delete_name, endpoint_id, deletable_paths)

def globus_delete_paths(tc, delete_name, endpoint_id, paths):
    """Delete the objects at the designated paths from the given endpoint and return the response.

    Keyword Arguments:
    tc -- a transfer client, necessary to perform a delete
    delete_name -- a name for the delete
    endpoint_id -- the ID of the endpoint at which the objects are located
    paths -- a list of paths to the objects
    """
    if paths:
        ddata = globus_sdk.DeleteData(tc, endpoint_id, label=delete_name, recursive=True)
        for path in paths:
            ddata.add_item(path)
        return tc.submit_delete(ddata)

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

def globus_get_file_structure(tc, endpoint_id, top_dir):
    """Return the file structure of the given directory at the given endpoint in an object of the 
    Directory class.

    Keyword Arguments:
    tc -- a transfer client, necessary to get the file structure at an endpoint
    endpoint_id -- the ID of the endpoint at which the file structure is located
    top_dir -- the top level directory whose file structure is to be retrieved
    """
    top_directory = Directory(top_dir)
    for entry in tc.operation_ls(endpoint_id, path=top_dir):
        abs_path = os.path.join(top_dir, entry["name"])
        if entry["type"] == "dir":
            top_directory.directories.append(globus_get_file_structure(tc, endpoint_id, abs_path))
        elif entry["type"] == "file":
            top_directory.files.append(abs_path)
    return top_directory

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

def globus_list_files(tc, endpoint_id, top_dir):
    """Return a list of the absolute paths to every file in every directory of the designated top 
    level directory.

    Keyword Arguments:
    tc -- a transfer client, necessary to perform a delete
    endpoint_id -- the ID of the endpoint at which the directory is located
    top_dir -- the top level directory whose files are to be listed
    """
    file_paths = []
    for entry in tc.operation_ls(endpoint_id, path=top_dir):
        abs_path = os.path.join(top_dir, entry["name"])
        if entry["type"] == "file":
            file_paths.append(abs_path)
        elif entry["type"] == "dir":
            file_paths.extend(globus_list_files(tc, endpoint_id, abs_path))
    return file_paths

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

def globus_transfer_name(transfer_type):
    """Return a name for a Globus transfer, consisting of the time.

    Keyword Arguments:
    type -- 0 for a transfer, 1 for a delete
    """
    if transfer_type not in [0, 1]:
        return "ERROR"
    if transfer_type == 0:
        type_name = "TRANSFER_"
    else:
        type_name = "DELETE_"
    return "AUTO_" + type_name + datetime.now().strftime("%Y-%m-%d_%H%M")

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
