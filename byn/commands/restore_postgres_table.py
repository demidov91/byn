import asyncio
import gzip
import sys
import shutil
import logging
import os
import csv

from byn.postgres_db import connection


logger = logging.getLogger(__name__)


async def run(table_name: str, path: str, cleanup=False):
    if path.endswith('.csv.gz'):
        with gzip.open(path, mode='rb') as orig, open(path[:-3], mode='wb') as dest:
            shutil.copyfileobj(orig, dest)

        path = path[:-3]
        cleanup = True

    elif not path.endswith('.csv'):
        raise ValueError('Unexpected dump file extension: %s' % path)

    with open(path, mode='rt', newline='') as f:
        headers = next(csv.reader(f, delimiter=';'))

    logger.info('Start restoring %s', table_name)

    async with connection() as conn:
        await conn.execute(
            f"COPY {table_name}({', '.join(headers)}) from %s DELIMITER ';' CSV HEADER",
            path
        )

    logger.info('Table %s is restored.', table_name)

    if cleanup:
        os.remove(path)


if __name__ == '__main__':
    asyncio.run(run(sys.argv[1], sys.argv[2]))
