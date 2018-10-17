#!/usr/bin/env python

import globus_sdk
import logging
import os
from datetime import datetime
from enum import Enum

"""Utilities."""

__author__ = "Matthew E. Li"
__email__ = "meli@lbl.gov"

class DirectoryObject(Enum):
    """An object containing names corresponding to Directory objects."""

    DIR = "directory"
    FILE = "file"

    @classmethod
    def is_valid(cls, obj):
        """Returns whether or not the given object is a valid DirectoryObject.

        Keyword Arguments:
        cls -- the class object
        obj -- the object to check
        """
        return hasattr(obj, "value") and any(obj.value == item.value for item in cls)

class DirectoryNode(object):
    """An object representing a directory, storing contained entries and data."""

    def __init__(self):
        """Instantiates a DirectoryNode object, defining contained entries and data.

        Keyword Arguments:
        self -- the class object
        """
        self.entries = {}
        self.type = None
        self.data = None

    def iterator(self, path=""):
        """Returns a generator over the contents of the directory at the given path, where each 
        outputted entry is a tuple of the form (absolute_path, type, data).

        Keyword Arguments:
        self -- the class object
        path -- a path given to facilitate the generation of the absolute path
        """
        for entry in self.entries:
            node = self.entries[entry]
            absolute_path = os.path.join("/", os.path.join(path, entry))
            yield absolute_path, node.type, node.data
            if node.entries:
                yield from node.iterator(path=absolute_path)

class DirectoryTrie(object):
    """A trie representing a directory structure, containing DirectoryNode objects."""

    def __init__(self):
        """Instantiates a DirectoryTrie object, defining a root DirectoryNode.

        Keyword Arguments:
        self -- the class object
        """
        self.root = self.get_node()

    def find(self, path):
        """Returns whether or not the DirectoryTrie contains an entry for the given path, as well 
        as the data contained in the found node if it does.

        Keyword Arguments:
        self -- the class object
        path -- the path being searched for
        """
        node = self.root
        entries = [entry.strip() for entry in path.split("/") if entry.strip()]
        for i in range(len(entries)):
            entry = entries[i]
            if entry in node.entries:
                node = node.entries[entry]
            else:
                return False, None, None
        return True, node.type, node.data

    def get_node(self):
        """Returns a new DirectoryNode object.

        Keyword Arguments:
        self -- the class object
        """
        return DirectoryNode()

    def get_node_from_path(self, path):
        """Returns the node corresponding to the given path or None if not found.

        Keyword Arguments:
        self -- the class object
        path -- the path corresponding to the node to find
        """
        node = self.root
        entries = [entry.strip() for entry in path.split("/") if entry.strip()]
        for entry in entries:
            try:
                node = node.entries[entry]
            except KeyError as e:
                return None
        return node

    def insert(self, path, node_type, data):
        """Inserts the given path of the given type into the DirectoryTrie with the given data.

        Keyword Arguments:
        self -- the class object
        path -- the path to insert
        node_type -- the type of object the path represents
        data -- the data to be stored at the given path
        """
        if not DirectoryObject.is_valid(node_type):
            raise TypeError("Invalid object type {}.".format(node_type))
        node = self.root
        entries = [entry.strip() for entry in path.split("/") if entry.strip()]
        index = None
        for i in range(len(entries)):
            entry = entries[i]
            if entry in node.entries:
                node = node.entries[entry]
            else:
                index = i
                break
        if index is not None:
            for i in range(index, len(entries)):
                entry = entries[i]
                node.entries[entry] = self.get_node()
                node = node.entries[entry]
        node.type, node.data = node_type, data

    def set_data_recursive(self, path, data):
        """Sets the data for the node corresponding to the given path. If the path points to a 
        directory, the times for its files and subdirectories are recursively set as well.

        Keyword Arguments:
        self -- the class object
        path -- the absolute path to a node
        """
        node = self.get_node_from_path(path)
        if node:
            node.data = data
            for entry in node.entries:
                self.set_data_recursive(os.path.join(path, entry), data)

class GlobusDirectoryTrie(DirectoryTrie):
    """A subclass of DirectoryTrie with additional functionality specific to Globus."""

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, top_dir):
        """Instantiates a GlobusDirectoryTrie object, defining a root DirectoryNode representing 
        the given top level directory.

        Keyword Arguments:
        self -- the class object
        top_dir -- the absolute path to the top level directory represented by the trie
        """
        self.root = self.get_node()
        self.top_dir = top_dir
        self.insert(self.top_dir, DirectoryObject.DIR, None)

    def add_new_paths(self):
        """Scans the directory and inserts newly added paths to the trie.

        Keyword Arguments:
        self -- the class object
        """
        for entry in scan_directory(self.top_dir):
            modified = last_modified(entry.path, GlobusDirectoryTrie.DATE_FORMAT)
            entry_data = self.find(entry.path)
            if not entry_data[0]:
                self.set_path_untransferred(entry.path)

    def get_transfer_paths(self):
        """Returns two sets of absolute paths to be transferred: one is a set of paths for 
        directories that need to be created and the other is a set of paths for files that need to 
        be transferred.
        """
        dirs_to_create, files_to_transfer = set(), set()
        for (absolute_path, node_type, last_transferred) in self.root.iterator():
            if node_type == DirectoryObject.DIR:
                if absolute_path.startswith(self.top_dir) and dir_exists(absolute_path):
                    if not os.listdir(absolute_path) and not last_transferred:
                        dirs_to_create.add(absolute_path)
            elif node_type == DirectoryObject.FILE:
                if absolute_path.startswith(self.top_dir) and file_exists(absolute_path):
                    modified = last_modified(absolute_path, GlobusDirectoryTrie.DATE_FORMAT)
                    if not last_transferred or last_transferred < modified:
                        files_to_transfer.add(absolute_path)
        return dirs_to_create, files_to_transfer

    def set_path_untransferred(self, path):
        """Marks the entry in the GlobusDirectoryTrie for the given path as not transferred.

        Keyword Arguments:
        self -- the class object
        path -- the path to reset
        """
        last_transferred = None
        if os.path.exists(path):
            node_type = None
            if os.path.isdir(path):
                node_type = DirectoryObject.DIR
            elif os.path.isfile(path):
                node_type = DirectoryObject.FILE
            if node_type:
                self.insert(path, node_type, last_transferred)

    def set_transfer_times(self, paths, node_type):
        """For each path given, sets the time it was transferred to the current time, marked in 
        the trie as the given DirectoryObject type.

        Keyword Arguments:
        self -- the class object
        paths -- a list of absolute paths
        node_type -- a DirectoryObject choice
        """
        if not DirectoryObject.is_valid(node_type):
            raise TypeError("Invalid object type {}.".format(node_type))
        transfer_time = datetime.now().strftime(GlobusDirectoryTrie.DATE_FORMAT)
        for path in paths:
            self.insert(path, node_type, transfer_time)

def dir_exists(dir_path):
    """Checks whether or not the object at the given path is an existing directory.

    Keyword Arguments:
    dir_path -- the path to check
    """
    return os.path.exists(dir_path) and os.path.isdir(dir_path)

def file_exists(file_path):
    """Checks whether or not the object at the given path is an existing file.

    Keyword Arguments:
    file_path -- the path to check
    """
    return os.path.exists(file_path) and os.path.isfile(file_path)

def datetime_now(date_format):
    """Returns the current time in the given format.

    Keyword Arguments:
    date_format -- the string representation the date should be formatted in
    """
    return datetime.now().strftime(date_format)

def get_logger(name, log_path):
    """Returns a logger for the given name that writes to the given path.

    Keyword Arguments:
    name -- the name to which the log corresponds
    log_path -- the path to the log file
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

def get_src_dst_pairs(paths, src_prefix, dst_prefix):
    """Returns a mapping from absolute source path to absolute destination path, where the 
    destination prefix replaces the source prefix in each path.

    Keyword Arguments:
    paths -- a set containing absolute paths in the source directory
    src_prefix -- the prefix of the path in the source directory
    dst_prefix -- the prefix of the path in the destination directory
    """
    pairs = {}
    paths = list(paths)
    for i in range(len(paths)):
        src_path = paths[i]
        dst_path = replace_path_prefix(src_path, src_prefix, dst_prefix)
        if src_path and dst_path:
            pairs[src_path] = dst_path
    return pairs

def get_user_datetime(date_format):
    """Returns a datetime object in the given format from user input, including year, month, day, 
    optional hour, and optional minute. The inputted time must be earlier than the current time.

    Keyword Arguments:
    date_format -- the format in which the date will be returned
    """
    year = int(input("Year: "))
    month = int(input("Month: "))
    day = int(input("Day: "))
    hour = input("Hour (Optional): ")
    hour = int(hour) if hour.strip() else 0
    minute = input("Minute (Optional): ")
    minute = int(minute) if minute.strip() else 0
    user_datetime = datetime(year=year, month=month, day=day, hour=hour, minute=minute)
    if user_datetime > datetime.now():
        raise ValueError("The date must be in the past.")
    return user_datetime.strftime(date_format)

def globus_create_dirs(tc, endpoint_id, paths):
    """Creates directories at the given paths at the endpoint with the given ID.

    Keyword Arguments:
    tc -- a transfer client, necessary to create directories
    endpoint_id -- the ID of the endpoint
    paths -- a list containing absolute paths at which to create directories
    """
    for path in paths:
        try:
            tc.operation_mkdir(endpoint_id, path=path)
        except:
            pass

def globus_endpoint_ready(tc, endpoint_id):
    """Checks that an endpoint is ready for transfer.

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
    """Generates a refresh token for the given Globus Auth client having the given application ID 
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
    """Generates and returns a TransferClient by initializing an NativeAppAuthClient and a 
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

def globus_transfer_files(tc, transfer_name, src_id, dst_id, path_pairs):
    """Transfers files from source to destination.

    Keyword Arguments:
    tc -- a transfer client, necessary to perform a transfer
    transfer_name -- a name for the transfer
    src_id -- the ID of the source endpoint
    dst_id -- the ID of the destination endpoint
    path_pairs -- a mapping from source path to destination path
    """
    if path_pairs:
        try:
            tdata = globus_sdk.TransferData(tc, src_id, dst_id, label=transfer_name, 
                                            sync_level="mtime", preserve_timestamp=True, 
                                            encrypt_data=True)
            for src_path, dst_path in path_pairs.items():
                tdata.add_item(src_path, dst_path)
            return tc.submit_transfer(tdata)
        except Exception as e:
            raise e

def globus_transfer_name(date_format):
    """Returns a name for a Globus transfer, consisting of the time in the given format.
    
    Keyword Arguments:
    date_format -- the string representation the date should be formatted in
    """
    return "AUTO_" + datetime_now(date_format)

def last_modified(path, date_format):
    """Returns a timestamp, in the given format, at which the object at the given path was last 
    modified.

    Keyword Arguments:
    path -- the path to the object
    date_format -- the string representation the date should be formatted in
    """
    if os.path.exists(path):
        return datetime.fromtimestamp(os.stat(path).st_mtime).strftime(date_format)
    return None

def replace_path_prefix(path, old_prefix, new_prefix):
    """Replaces the prefix of the given path with a new one. Returns None if the given path does 
    not begin with the given old prefix.

    Keyword Arguments:
    path -- the path to change
    old_prefix -- the prefix the path currently begins with
    new_prefix -- the prefix the path will begin with
    """
    old_prefix = os.path.join(old_prefix, "")
    if path and path.startswith(old_prefix):
        return os.path.join(new_prefix, path[len(old_prefix):])
    return None

def scan_directory(dir_path):
    """Returns a generator over the contents of the directory at the given path, including all its 
    subdirectories.

    Keyword Arguments:
    dir_path -- the path to the directory
    """
    for entry in os.scandir(dir_path):
        if entry.is_dir(follow_symlinks=False):
            yield entry
            yield from scan_directory(entry.path)
        elif entry.is_file(follow_symlinks=False):
            yield entry

def validate_user_path(path):
    """Returns a validated version of the given path, provided by the user. Raises an exception 
    if the path does not exist or is not absolute.

    Keyword Arguments:
    path -- the path to validate
    """
    path = path.strip()
    if not (os.path.exists(path) and os.path.isabs(path)):
        raise FileNotFoundError("Please enter an existing absolute path.")
    return path
