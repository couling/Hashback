# ⚠️ Project Status ⚠️

⚠️ Hashback is still in ALPHA. ⚠️

⚠️ Do NOT use this for critical backups! ⚠️

# Hashback
Hashback is a backup system.

It's designed to take snapshot backups of multiple devices (eg: multiple servers) Using only the storage you might 
expect for incremental backups.

Hashback indexes files and directories by the sha256 hash of their content.  Metadata data such as file ownership and 
file names are stored in the directory listings, not the file backups allowing files to be moved around and renamed 
without duplicating the file content, even when moving across devices.

# Configure your server

You can install with either: 
 - Docker ([instructions](https://github.com/couling/Hashback/tree/main/docs/user/install_server_with_docker.md)) 
 - Python ([instructions](https://github.com/couling/Hashback/tree/main/docs/user/install_server_with_python))

# Configure your client

The client can be run through docker-compose as well.  Just remember to bind mount your data directory in so that it can
be backed up.

See the output from authorizing your client for these values:

    # See installing server instructions for credentials

    hashback configure \
        --client-id 3bed0c4f-a1c9-4f37-b2b5-8b393c190a40 \
        --database-url https://example.com/ \
        --credentials '{
            "auth_type": "basic", 
            "username": "3bed0c4f-a1c9-4f37-b2b5-8b393c190a40", 
            "password": "fa72112f-a9a8-4be2-b293-124bf875f86c"
          }'
        


# This repo includes

## `hashback` (client) command line tool

This is the backup client intended for taking frequent snapshots systems as well as restoring backups.

hashback can be used to backup to a local database without using a server or it can backup to a remote server.

## `hashback-db-admin` admin commandline tool

This is used to create and manage backup databases.  This is initially required to setup a device in the database before
that device can begin to backup.

## `hashback-basic-server` a simple http backup server

This is intended as an MVP server.  It only supports http auth type basic.  It is highly recommended to sit this behind 
https reverse gateway such as [Traefik](https://traefik.io/).
