# Databricks PySpark Workspace Sync and Run

This tool is to allow users to sync local files and modules to the Databricks environment.  
This will kick off a single run on an existing cluster if one exists, and provide the run URL.  

```bash
usage: sync_pyspark.py [-h] [--user USER] [--password PASSWORD] [--host HOST]
                       --dir DIR [--cluster CLUSTER]
                       main_file

Iterative development script for Databricks Execution e.g.  
$ python sync_pyspark.py --prefix "test" main_file.py  
Given environment variables are set for the platform, you can quickly push the main files with existing modules. 
This assumes all other python files are to be imported in this directory.

positional arguments:
  main_file            The path/directory in Databricks or locally to sync

optional arguments:
  -h, --help           show this help message and exit
  --user USER          Username for the Databricks env
  --password PASSWORD  Password for the Databricks env
  --host HOST          Password for the Databricks env
  --dir DIR            The path/directory in Databricks to push the source
                       files sync
  --cluster CLUSTER    Regex of partial cluster name to attach to and run the
                       job.
```
