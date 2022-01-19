# Installing Server with Python

## 1 Install the software

In an empty directory create a new virtual environment named `venv`.

```shell
# Make a new directory an switch into it.
mkdir hashback
cd hashback

# Create a virtual envrionment
python3 -m venv venv

# Activate the environment
source venv/bin/activate

# Install hashback
pip install hashback
```

If you come back later and wish to run some of these commands again you must again:

    source venv/bin/activate


## 2 Create a backup database

Create a backup database wherever you want it to go (eg: `/backup_db`)

```
hashback-db-admin /backup_db create
```

## 3 Configure the server

Copy the [example server config file](https://github.com/couling/Hashback/tree/main/docs/examples/basic-server.json).
This needs to go in a standard location.  On linux thats either`/etc/hashback/basic-server.json` or in your home 
directory: `$HOME/.conf/hashback/basic-server.json`.

Update that file to change `database_path` to the directory containing your backup database (eg: `/backup_db`).

This file will be used later automatically by `hashback-basic-server`

## 4 Add a new client to the database

Every client is given a name.  In this example the client is named `bob`.  It's also given an ID which is generated automatically.

    hashback-db-admin /backup_db add-client bob

## 5 Authorize the client on the server

    hashback-basic-server authorize bob

Carefully copy the output, it includes the credentials for your new client.  The output should look something like this:

```
2022-01-19 20:49:59,658 - hashback.basic_auth.server - INFO - Authorizing Client: bob (3bed0c4f-a1c9-4f37-b2b5-8b393c190a40)
2022-01-19 20:49:59,659 - root - INFO - Credentials for bob: 
{"auth_type": "basic", "username": "3bed0c4f-a1c9-4f37-b2b5-8b393c190a40", "password": "fa72112f-a9a8-4be2-b293-124bf875f86c"}
2022-01-19 20:49:59,663 - hashback.basic_auth.basic_auth - INFO - User 3bed0c4f-a1c9-4f37-b2b5-8b393c190a40 created
```

The above output shows the client `bob` is client_id `3bed0c4f-a1c9-4f37-b2b5-8b393c190a40` and has credentials:

```
{
    "auth_type": "basic", 
    "username": "3bed0c4f-a1c9-4f37-b2b5-8b393c190a40", 
    "password": "fa72112f-a9a8-4be2-b293-124bf875f86c"
}
```