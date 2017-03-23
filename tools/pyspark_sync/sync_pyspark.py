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
            if (line.startswith('# MAGIC ')) and (import_file.rstrip('.py') in line):
                return False
    os.rename(main_file, 'tmp_' + main_file)
    with open(main_file, 'w') as f:
        f.write(magic_string.format('./' + import_file.rstrip('.py')))
        with open('tmp_' + main_file) as r:
            for line in r:
                f.write(line)
    return True


def get_cluster_id(client, cluster_name):
    cl = client.get('/clusters/list')['clusters']
    live_clusters = WorkspaceClient.my_map(lambda y: (y['cluster_name'], y['cluster_id']), \
                        filter(lambda x: x['state'] == 'RUNNING' or x['state'] == 'RESIZING', cl))
    #print(live_clusters)
    for (cn, cid) in live_clusters:
        if cluster_name.lower() in cn.lower():
            return cn, cid
    raise NameError('Cluster name does not exist')


def get_job_id(client, job_name):
    job_list = client.get('/jobs/list')['jobs']
    for i in job_list:
        if i['settings']['name'] == job_name:
            return i['job_id']
    return None


def create_job_and_run(main_notebook, cluster_name, client):
    """ create job template and kick off run-now option on a running cluster.
    throw error if cluster does not exist"""
    (cname, cid) = get_cluster_id(client, cluster_name)
    print(cname, cid)
    # get notebook path in Databricks
    notebook = '/Users/' + client.user.strip() + '/' + main_notebook.lstrip('./').rstrip('.py')
    job_name = notebook.split('/')[-1]
    create_new_job = {
        "name": job_name,
        "existing_cluster_id": cid,
        "notebook_task": {
            "notebook_path": notebook
        },
        "timeout_seconds": 1800
    }
    job_id = get_job_id(client, job_name)
    if job_id is None:
        print("Job does not exist. Create new job: ")
        resp = client.post('/jobs/create', create_new_job)
        job_id = resp['job_id']
        # create the job config here
    else:
        print("Updating existing job config")
        resp = client.get('/jobs/get?job_id={0}'.format(job_id))
        cur_config = resp.pop('settings')
        cur_name = cur_config['name']
        # remove the name setting from above, and create a new_settings config to point to existing configs
        create_new_job['name'] = cur_name
        update_config = resp
        update_config['new_settings'] = create_new_job
        resp = client.post('/jobs/reset', update_config)
    # kick off a single run of this job
    run_now_params = { 'job_id': job_id }
    resp = client.post('/jobs/run-now', run_now_params)
    print(resp)
    resp['job_id'] = job_id
    return resp


if __name__ == '__main__':
    debug = False
    parser = argparse.ArgumentParser(description="""
    Iterative development script for Databricks Execution
    e.g.
    $ python sync_pyspark.py --prefix "test" main_file.py
    Given environment variables are set for the platform, you can quickly push the main files with its supporting files
    This assumes all other python files are to be imported.
    """)
    parser.add_argument('--user', dest='user', help='Username for the Databricks env')
    parser.add_argument('--password', dest='password', help='Password for the Databricks env')
    parser.add_argument('--host', dest='host', help='Password for the Databricks env')

    parser.add_argument('--dir', required='true', dest='dir', type=str,
                        help='The path/directory in Databricks to push the source files sync')
    parser.add_argument('--cluster', dest='cluster', type=str,
                        help='Regex of partial cluster name to attach to and run the job.')
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
        if os.path.exists(helper_file):
            os.remove(helper_file)

        exclude_files = ['./workspace.py', './sync_pyspark.py']
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

        ####################
        ## Run on a cluster
        ####################
        if args.cluster:
            db_path = args.dir.rstrip('/') + '/' + args.main_file.lstrip('./')
            job_details = create_job_and_run(db_path, args.cluster, helper)
            job_run_url = helper.host + '/#job/' + str(job_details['job_id']) + '/run/' + str(job_details['number_in_job'])
            print("Job run url: \n" + job_run_url)
        else:
            print("Cluster argument not defined. Please run manually")
