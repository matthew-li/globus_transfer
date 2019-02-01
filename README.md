Globus Automated Transfer
==========================================

## Overview

A Python script that performs a single Globus transfer from a directory on a source endpoint to a directory on a destination endpoint. A transfer is comprised of the files and directories that were added or modified since the last transfer. This script can be scheduled as a cron job to automatically detect changes and initiate transfers.

This code originated from the Scientific Computing Group (SCG) at Lawrence Berkeley National Laboratory.

## Table of Contents

1. [How It Works](#1-how-it-works)
2. [Requirements](#2-requirements)
3. [Package Contents](#3-package-contents)
4. [Configuration](#4-configuration)
5. [Output](#5-output)
6. [Automation](#6-automation)

## 1. How It Works

The script uses a Python shelf named `datastore` that, for each file or empty directory in `SRC_DIR`, stores the timestamp of the last time the object was transferred. Paths are stored in a trie, where each node corresponds to a directory or file name, for space efficiency.

When `main.py` is run, `SRC_DIR` is scanned. Any new files or empty directories, since non-empty directories are automatically created on the destination side, are inserted into the trie. The trie is then iterated over, generating paths to be included in a new transfer.

The user can use `set_time.py` to artificially set the last transfer time for each path in `SRC_DIR`. This can be used to avoid transferring data older than some time, or to retry failed transfers. The user can also change the time of a specific path within `SRC_DIR` using `--path`. If it points to a directory, all its enclosed files and directories will have their times changed as well.

Once a Globus transfer is initiated, only those paths whose modification time is newer on the source endpoint than on the destination endpoint are transferred.

Updates are written to the log.

## 2. Requirements

Python 3.3+ and the package `globus-sdk` (`pip install globus-sdk`) are required.

A client application must be created under the user's Globus account to authorize transfers. Follow Steps 1-2 of this tutorial: https://globus-sdk-python.readthedocs.io/en/stable/tutorial/. Add the application's ID to `config.py`.

## 3. Package Contents

| File Name | Description |
| --------- | ----------- |
| `config.py` | Configuration, including Globus parameters and paths. |
| `__init__.py` | An empty file that denotes that the directory is a Python package. |
| `main.py` | The main program that initiates a transfer. |
| `set_time.py` | A utility that allows the user to manually reset the timestamp for the last transfer for a given path. |
| `test_config.py` | A test that the configuration is valid and that transfer is possible. |
| `utils.py` | Utility functions. |

## 4. Configuration

Below are the configuration variables the user will need to set.

| Variable | Description |
| -------- | ----------- |
| `SRC_DIR` | The absolute path to the source directory in the source endpoint. |
| `DST_DIR` | The absolute path to the destination directory in the destination endpoint. Globus transfers only the contents of `SRC_DIR` and not the enclosing `SRC_DIR` itself, so the user should include the name of the deepest directory in `SRC_DIR` at the end of `DST_DIR`. |
| `SRC_ID` | The ID of the source endpoint, which can be found on the Globus website. |
| `DST_ID` | The ID of the destination endpoint, which can be found on the Globus website. |
| `CLIENT_ID` | The ID for the Globus client application authorizing transfers, which can be found at https://developers.globus.org once a client application has been created. |
| `CODE_PATH` | The absolute path to the code package. |

## 5. Output

| File Name | Description |
| --------- | ----------- |
| `datastore*` | A Python shelf that stores metadata associated with files, specifically the timestamp at which each file or directory was last transferred. |
| `log` | A log that the script writes to. |

## 6. Automation

The script can be scheduled in a cron job to run regularly without user interaction. Below is the recommended setup:

1. Remove any existing `datastore` or `log` files.
2. Run `set_time.py` to set a time for all paths in `SRC_DIR`. Only paths that are added or modified after this time are considered for transfer. Each time the script is run, this time is updated for each path that is transferred. If `set_time.py` is not run, when `main.py` is run, all paths will be considered.
3. Run `test_config.py` once to test that the configuration is valid, that a refresh token for authorization exists, and that endpoints are ready. If no token exists, the user will be prompted to enter a code from a given link to generate one. Once it is generated, future transfers will not require authorization.
4. Set up a cron job to run the script periodically. `crontab -e` opens a VIM session that edits the `crontab`. See https://crontab.guru/examples.html for examples.
5. If failures arise, check the `log` and use `set_time.py` appropriately.
6. To stop automation, remove the line corresponding to the script from the `crontab`.
