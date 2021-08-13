# Hashback
Hashback is a part-written backup system including backup server and command-line backup tools.

It was inspired by the need to backup multiple servers with shared content that can often be moved around and renamed.
In such situations there is often a lot of duplication in the backup taking up space on disk and wasting a lot of time 
during the backup window.

Hashback indexes by the sha256 hash of their content.  Metadata and file names is stored in the directory listings and 
they are themselves hashed and indexed the same way.  Ultimately this allows things to be renamed, and metadata modified 
without storing massive repetition in the database.

## This repo includes

### Hasback command line tool

This creates backups locally and can be set on a schedule to run unassisted.  Backup configuration is stored server side
so there's no need to log into every client to update their configuration.

### Hashback db_admin tool

Besides the obvious things like creating a new backup database, this tool can also be used to seed backups by copying or 
even hardlinking a file tree into the local database.  *Hardlinking should be used with great care.*

### Hashback HTTP server

Hashback has it's own HTTP protocol for storing backups.  The hashback HTTP server implements that protocol using 
FastAPI and, by default, running with gunicorn.