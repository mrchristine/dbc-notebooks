# Databricks Workspace Tool

Here's the help text for the workspace API that allows users to sync the Databricks notebooks to their local directory, and allow integration with any git service.   

```bash
usage: workspace.py [-h] [--user USER] [--password PASSWORD] [--host HOST]
                    [--shared]
                    {push,pull} ... path

Sync Databricks workspace to/from local directory for git support. e.g.  
$ python workspaces.py pull demo/reddit/  
$ python workspaces.py push demo/reddit/ 
$ python workspaces.py pull --host='https://myenv.cloud.databricks.com/ --user=mwc@databricks.com --password=... ..  
I personally use the environment variables to store this information DBC_HOST DBC_USERNAME DBC_PASSWORD DBC_SHARED 
DBC_SHARED is set to true if the single repo needs to host multiple home directories. 
It creates a local directory from the users e-mail

positional arguments:
  {push,pull}
    push               Push path to Databricks workspace
    pull               Pull workspace from Databricks to local directory
  path                 The path/directory in Databricks or locally to sync

optional arguments:
  -h, --help           show this help message and exit
  --user USER          Username for the Databricks env
  --password PASSWORD  Password for the Databricks env
  --host HOST          Password for the Databricks env
  --shared             Boolean to notify if this is a shared repo to add a
                       username prefix to the directories
```
