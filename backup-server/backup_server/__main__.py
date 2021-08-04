from logging import DEBUG
from backup_server.misc import setup_logging, register_clean_shutdown
from backup_server.db_admin import main


if __name__ == '__main__':
    register_clean_shutdown()
    setup_logging()
    main()
