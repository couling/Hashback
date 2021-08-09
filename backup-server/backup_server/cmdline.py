import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import click
import dateutil.tz

from .misc import run_then_cancel, str_exception
from .protocol import ServerSession
from .scanner import Scanner

logger = logging.getLogger(__name__)


@click.group()
@click.option("--database", envvar="BACKUP_DATABASE")
@click.pass_context
def main(ctx: click.Context, database: str):
    ctx.obj = select_database(database)

@main.command("backup")
@click.argument('CLIENT_NAME', envvar="CLIENT_NAME")
@click.option("--timestamp", type=click.DateTime(formats=['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']))
@click.option("--description")
@click.option("--overwrite/--no-overwrite", default=False)
@click.pass_obj
def backup(server_session: ServerSession, timestamp: datetime, description: Optional[str],
           overwrite: bool):
    if timestamp is None:
        timestamp = datetime.now(dateutil.tz.gettz())
    elif timestamp.tzinfo is None:
        timestamp = timestamp.astimezone(dateutil.tz.gettz())

    async def _backup():
        backup_session = await server_session.start_backup(
            backup_date=timestamp,
            allow_overwrite=overwrite,
            description=description,
        )

        logger.info(f"Backup - {backup_session.config.backup_date}")
        backup_scanner = Scanner(backup_session)
        try:
            await backup_scanner.scan_all()
            logger.info("Finalizing backup")
            await backup_session.complete()
            logger.info("All done")
        except (Exception, asyncio.CancelledError) as ex:
            logger.warning("Discarding session (%s)", str_exception(ex))
            await backup_session.discard()
            raise

    run_then_cancel(_backup())


def select_database(path: str) -> ServerSession:
    url = urlparse(path)
    if url.scheme == '' or url.scheme == 'file':
        import local_database
        return local_database.LocalDatabase(Path(path)).open_client_session(client_id=url.username)
    elif url.scheme == 'http' or url.scheme =='https':
        import http_client, http_protocol
        server_properties = http_protocol.ServerProperties.parse_url(path)
        return asyncio.run(http_client.Client.login(server=server_properties))
