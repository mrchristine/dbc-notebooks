# Databricks notebook source
import base64
import argparse
import json
import requests
import sys
import os
import fnmatch

WS_LIST = "/workspace/list"
WS_STATUS = "/workspace/get-status"
WS_MKDIRS = "/workspace/mkdirs"
WS_IMPORT = "/workspace/import"
WS_EXPORT = "/workspace/export"


class WorkspaceClient:
    """A class to define wrappers for the REST API"""

    def __init__(self, host="https://myenv.cloud.databricks.com", user="admin", pwd="fakePassword"):
        self.user = user
        self.pwd = pwd
        self.creds = (user, pwd)
        self.host = host
        self.url = host.rstrip('/') + '/api/2.0'

    def get(self, endpoint, json_params={}, print_json=False):
        url = self.url + endpoint
        if json_params:
            results = requests.get(url, auth=self.creds, params=json_params).json()
        else:
            results = requests.get(url, auth=self.creds).json()
        if print_json:
            print(json.dumps(results, indent=4, sort_keys=True))
        return results

    def post(self, endpoint, json_params={}, print_json=True):
        url = self.url + endpoint
        if json_params:
            raw_results = requests.post(url, auth=self.creds, json=json_params)
            results = raw_results.json()
        else:
            print("Must have a payload in json_args param.")
            return {}
        if print_json:
            print json.dumps(results, indent=4, sort_keys=True)
        # if results are empty, let's return the return status
        if results:
            results['http_status_code'] = raw_results.status_code
            return results
        else:
            return {'http_status_code': raw_results.status_code}

    def is_file(self, path):
        """ Checks if the file is a notebook or folder in Databricks"""
        status = {'path': path}
        resp = self.get(WS_STATUS, json_params=status)
        print("Is the path a file or folder: ")
        print(resp)
        if resp['status']['entityType'] == 'Folder':
            return False
        return True

    def get_full_path(self, path):
        """ Get the full path of the Databricks workspace
         User's can provide the relative path to push / pull from Databricks"""
        if path[0] == '/':
            # return path is absolute so return here
            return path
        elif path[0] == '.':
            full_path = '/Users/' + self.user.strip() + path[1:]
            return full_path
        else:
            raise ValueError('Path should start with . for relative paths or / for absolute.')

    def save_single_notebook(self, fullpath):
        """ Saves a single notebook from Databricks to the local directory"""
        get_args = {'path': fullpath}
        resp = self.get(WS_EXPORT, get_args)
        # grab the relative path from the constructed full path
        # this code chops of the /Users/mwc@example.com/ to create a local reference
        save_filename = '/'.join(fullpath.split('/')[3:]) + '.' + resp['fileType']
        save_path = os.path.dirname(save_filename)
        print("Saving file in local path: " + save_filename)
        # If the local path doesn't exist,we create it before we save the contents
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        with open(save_filename, "w") as f:
            f.write(base64.b64decode(resp['content']))

    @staticmethod
    def find_relative_path(self, folder, fullpath):
        """ Split the full path to find the folder index to create a local dir
        e.g. /Users/mwc@databricks.com/demo/reddit/
        output: demo/reddit/
        Should check that the folder is a workspace folder"""
        path_array = fullpath.split('/')
        folder_indx = path_array.index(folder)
        rel_path = '/'.join(path_array[:folder_indx][3:])
        if not rel_path:
            return './'
        return rel_path + '/'

    def get_all_notebooks(self, fullpath):
        """ Recursively list all notebooks within the folder"""
        get_args = {'path': fullpath}
        items = self.get(WS_LIST, get_args)['entities']
        folders = map(lambda y: y.get('path', None),
                      filter(lambda x: x.get('entityType', None) == 'Folder', items))
        notebooks = map(lambda y: y.get('path', None),
                        filter(lambda x: x.get('entityType', None) == 'Notebook', items))
        print 'Folders: ' + str(folders)
        print 'Notebooks: ' + str(notebooks)
        if folders == [] and notebooks == []:
            raise ValueError('Folder does not contain any notebooks')
        # save the notebooks with the current method
        if notebooks:
            map(lambda y: self.save_single_notebook(y), notebooks)
        if folders:
            nested_list_notebooks = map(lambda y: self.get_all_notebooks(y), folders)
            flatten_list = [item for sublist in nested_list_notebooks for item in sublist]
            return notebooks + flatten_list
        return notebooks

    def save_folder(self, fullpath):
        """ We will save the notebooks within the paths, and exclude Library links """
        list_of_notebooks = self.get_all_notebooks(fullpath)
        return list_of_notebooks
        # Run map of save_single_notebook across list of notebooks

    def pull(self, path):
        # get_args = "/Users/mwc@databricks.com/demo/reddit/Reddit SQL Analysis"
        cur_path = self.get_full_path(path)

        # pull the file or archive
        if self.is_file(cur_path):
            self.save_single_notebook(cur_path)
        else:
            self.save_folder(cur_path)

    @staticmethod
    def _parse_extension(src_path):
        supported = ['scala', 'py', 'r', 'sql']
        ext = src_path.split('.')[-1]
        if ext == 'scala':
            return {'language': 'Scala'}
        elif ext == 'py':
            return {'language': 'Python'}
        elif ext == 'ipynb':
            return {'format': 'IPython'}
        elif ext == 'r':
            return {'language': 'R'}
        elif ext == 'sql':
            return {'language': 'SQL'}
        elif ext == 'txt':
            return {'language': 'SQL'}
        else:
            raise ValueError('Unsupported file format: %s. Supported formats are: ' % ext +
                             '[%s].' % ', '.join(supported))

    def push_file(self, local_path):
        """Push a single file to DBC
          This assumes the local path matches the Databricks workspace"""
        # get the databricks path using the users hostname
        tmp_path = '/Users/' + self.user.strip() + '/' + local_path.lstrip('./')
        overwrite = True
        dirname = os.path.dirname(tmp_path)
        dbc_path, file_ext = os.path.splitext(tmp_path)
        data = open(local_path, 'r').read()
        create_notebook = {
           "path": dbc_path,
           "content": base64.b64encode(data),
           "overwrite": overwrite
        }
        create_notebook.update(self._parse_extension(local_path))
        # create a folder, if exists then it succeeds as well
        folder_resp = self.post(WS_MKDIRS, {'path': dirname}, False)
        # import the notebook
        resp = self.post(WS_IMPORT, create_notebook, False)
        print "Push Notebook: " + dbc_path
        print(resp)

    @staticmethod
    def find_all_file_paths(local_dir):
        matches = []
        supported = ['scala', 'py', 'r', 'sql']
        for root, dirnames, filenames in os.walk(local_dir):
            for ext in supported:
                for filename in fnmatch.filter(filenames, '*.' + ext):
                    matches.append(os.path.join(root, filename))
        return matches

    def push_folder(self, local_path):
        """ Find all source files first, grab all the folders, batch create folders, push notebooks"""
        file_list = self.find_all_file_paths(local_path)
        cwd = os.getcwd()
        file_list_rel_path = map(lambda x: x.replace(cwd, "."), file_list)
        for fname in file_list_rel_path:
            self.push_file(fname)
        return file_list_rel_path

    def push(self, path):
        if path[0] == '/':
            raise ValueError("Path should be relative to your git repo home dir and start with ./ or with folder name")
        if os.path.isfile(path):
            self.push_file(path)
        else:
            self.push_folder(path)


if __name__ == '__main__':
    debug = False
    parser = argparse.ArgumentParser(description='Sync Databricks workspace to/from local directory for git support. '
                                                 'e.g. --users=\'mwc@databricks.com\'')
    # subparser for mutually exclusive arguments
    sp = parser.add_subparsers(dest='action')
    sp_push = sp.add_parser('push', help='Push path to Databricks workspace')
    sp_pull = sp.add_parser('pull', help='Pull workspace from Databricks to local directory')

    parser.add_argument('--user', dest='user', help='Username for the Databricks env')
    parser.add_argument('--password', dest='password', help='Password for the Databricks env')
    parser.add_argument('--host', dest='host', help='Password for the Databricks env')

    parser.add_argument('--shared', dest='shared', action='store_true',
                        help='Boolean to notify if this is a \
                        shared repo to add a username prefix to the directories')

    parser.add_argument('path', type=str,
                        help='The path/directory in Databricks or locally to sync')

    args = parser.parse_args()
    # the arguments
    user = args.user
    host = args.host
    password = args.password
    is_shared = args.shared
    print(args)
    if not host:
        host = os.environ.get('DBC_HOST')
    if not user:
        user = os.environ.get('DBC_USERNAME')
    if not password:
        password = os.environ.get('DBC_PASSWORD')
    if not is_shared:
        is_shared = bool(os.environ.get('DBC_SHARED'))
    helper = WorkspaceClient(host, user, password)

    if debug:
        print("ACTION IS: " + args.action)
        print("PATH IS: " + args.path)
        print("USER IS: " + user)
        print("PASS IS: " + "I_DONT_PRINT_PASSWORDS")
        print("HOST IS: " + host)
    if args.path is None:
        print("Need path")
        exit(0)
    else:
        input_path = args.path
        if args.action.lower() == "push":
            helper.push(input_path)
        elif args.action.lower() == "pull":
            helper.pull(input_path)
        else:
            print("Push / pull are only supported as the action.")
