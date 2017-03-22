from workspace import *

magic_string = """
# MAGIC %run "{0}"

# COMMAND ----------
"""


def create_helper_nb(list_of_modules, fname):
    # Need to prefix this in front of the file to import modules
    with open(fname, 'w') as f:
        for nb in list_of_modules:
            f.write(magic_string.format(nb.rstrip('.py')))


def push_to_databricks(client, dst_folder, list_of_files):
    for f in list_of_files:
        client.push_file(f, dst_folder)


def add_helper_to_main(main_file, import_file):
    with open(main_file, 'r') as v:
        for line in v:
            if import_file in line:
                return False
    os.rename(main_file, 'tmp_' + main_file)
    with open(main_file, 'w') as f:
        f.write(magic_string.format('./' + import_file.rstrip('.py')))
        with open('tmp_' + main_file) as r:
            for line in r:
                f.write(line)
    return True

if __name__ == '__main__':
    debug = False
    parser = argparse.ArgumentParser(description="""
    Iterative development script for Databricks Execution
    e.g.
    $ python run_in_db.py --prefix "test" main_file.py
    Given environment variables are set for the platform, you can quickly push the main files with its supporting files
    This assumes all other python files are to be imported.
    """)
    parser.add_argument('--user', dest='user', help='Username for the Databricks env')
    parser.add_argument('--password', dest='password', help='Password for the Databricks env')
    parser.add_argument('--host', dest='host', help='Password for the Databricks env')

    parser.add_argument('--dir', required='true', dest='dir', type=str,
                        help='The path/directory in Databricks to push the source files sync')
    parser.add_argument('main_file', type=str,
                        help='The path/directory in Databricks or locally to sync')

    args = parser.parse_args()
    # the arguments
    user = args.user
    host = args.host
    password = args.password
    if not host:
        host = os.environ.get('DBC_HOST')
    if not user:
        user = os.environ.get('DBC_USERNAME')
    if not password:
        password = os.environ.get('DBC_PASSWORD')
    helper = WorkspaceClient(host, user, password)

    if debug:
        print("ACTION IS: " + args.action)
        print("PATH IS: " + args.path)
        print("USER IS: " + user)
        print("HOST IS: " + host)
    if args.main_file is None:
        print("Need path")
        exit(0)
    else:
        input_path = args.dir.rstrip('/') + '/' + args.main_file
        helper_file = 'import_helper.py'
        os.remove(helper_file)

        exclude_files = ['./workspace.py', './run_in_db.py']
        all_helper_files = WorkspaceClient.find_all_file_paths('.')
        # remove the workspace client and run in db files from arguments
        for ef in exclude_files:
            all_helper_files.remove(ef)

        # copy the list of files
        files_to_push = list(all_helper_files)
        # remove the main job file from the helper list
        all_helper_files.remove('./' + args.main_file.lstrip('./'))

        # all helper source files that we will add to a helper file
        create_helper_nb(all_helper_files, helper_file)
        # add the helper file to files to push
        files_to_push.append('./' + helper_file)
        print('\nAll helper modules: ')
        print(all_helper_files)

        modified = add_helper_to_main(args.main_file, helper_file)
        if modified:
            print("Added helper to source file")
        else:
            print("Helper file already added to source file")

        push_to_databricks(helper, args.dir, files_to_push)
        # remove temp file
        try:
            os.remove('tmp_' + args.main_file)
        except:
            print("No temp file to clean")
        # Create helper notebook in local space
        print('\nPushing files to Databricks: ')
        print(files_to_push)

