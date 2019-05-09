import asyncio
import datetime
import os

from byn.tasks.launch import app
from byn.postgres_db import metadata, create_connection
# To activate logging:
import byn.logging


import logging
logger = logging.getLogger(__name__)


@app.task
def _send_table_backup_to_s3(folder: str, table: str):
    import boto3

    s3 = boto3.client('s3')
    s3.upload_file(
        os.path.join(folder, table + '.csv.gz'),
        os.environ['BACKUP_BUCKET'],
        f'{table}_{datetime.date.today():%Y-%m-%d}.csv.gz'
    )


PSQL_FOLDER = '/tmp/dump/'


@app.task
def dump_table(table_name: str):
    async def _implementation():
        os.makedirs(PSQL_FOLDER, exist_ok=True)
        os.chmod(PSQL_FOLDER, 0o777)
        path = os.path.join(PSQL_FOLDER, f'{table_name}.csv.gz')
        if os.path.exists(path):
            os.remove(path)
            
        logger.info('Start dumping %s', table_name)

        async with create_connection(timeout=5*60) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"COPY {table_name} TO PROGRAM "
                    f"'gzip 1>{path} 2>{PSQL_FOLDER}{table_name}.log'"
                    " DELIMITER ';' CSV HEADER",
                )

        logger.info('%s dump is created.', table_name)

    asyncio.run(_implementation())


@app.task
def postgresql_backup():
    for table_name in metadata.tables.keys():
        (dump_table.si(table_name) | _send_table_backup_to_s3.si(PSQL_FOLDER, table_name))()
