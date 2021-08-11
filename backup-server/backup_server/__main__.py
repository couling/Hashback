from backup_server.cmdline import main
from backup_server.misc import setup_logging, register_clean_shutdown

if __name__ == '__main__':
    register_clean_shutdown()
    setup_logging()
    main()
