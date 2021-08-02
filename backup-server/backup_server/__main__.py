from backup_server.misc import setup_logging
from backup_server.db_admin import main
from logging import DEBUG

if __name__ == '__main__':
    setup_logging(DEBUG)
    main()
