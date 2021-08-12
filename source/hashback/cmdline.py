import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import click
import dateutil.tz

from .misc import run_then_cancel, register_clean_shutdown, setup_logging
from .protocol import ServerSession, DuplicateBackup
from .scanner import Scanner

logger = logging.getLogger(__name__)


def main():
    register_clean_shutdown()
    setup_logging()
    click_main()


@click.group()
@click.option("--database", envvar="BACKUP_DATABASE")
@click.pass_context
def click_main(ctx: click.Context, database: str):
    ctx.obj = select_database(database)

@click_main.command("backup")
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']))
@click.option("--description")
@click.option("--overwrite/--no-overwrite", default=False)
@click.option("--fast-unsafe/--slow-safe", default=True)
@click.pass_obj
def backup(server_session: ServerSession, timestamp: datetime, description: Optional[str], fast_unsafe: bool,
           overwrite: bool):
    if timestamp is None:
        timestamp = datetime.now(dateutil.tz.gettz())
    elif timestamp.tzinfo is None:
        timestamp = timestamp.astimezone(dateutil.tz.gettz())

    async def _backup():
        try:
            backup_session = await server_session.start_backup(
                backup_date=timestamp,
                allow_overwrite=overwrite,
                description=description,
            )
        except DuplicateBackup as ex:
            raise click.ClickException(f"Duplicate backup {ex}") from None

        logger.info(f"Backup - {backup_session.config.backup_date}")
        backup_scanner = Scanner(backup_session)
        try:
            await backup_scanner.scan_all(fast_unsafe=fast_unsafe)
            logger.info("Finalizing backup")
            await backup_session.complete()
            logger.info("All done")
        except:
            logger.warning("Discarding session")
            await backup_session.discard()
            raise

    run_then_cancel(_backup())


def select_database(path: str) -> ServerSession:
    url = urlparse(path)
    if url.scheme == '' or url.scheme == 'file':
        logger.debug("Loading local database plugin")
        import local_database
        return local_database.LocalDatabase(Path(path)).open_client_session(client_id_or_name=url.username)
    elif url.scheme == 'http' or url.scheme =='https':
        logger.debug("Loading http client plugin")
        from  . import http_client, http_protocol
        server_properties = http_protocol.ServerProperties.parse_url(path)
        return run_then_cancel(http_client.BasicAuthClient.login(server_properties=server_properties))
