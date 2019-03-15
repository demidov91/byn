import logging

import happybase.connection
import happybase.pool
from happybase.connection import Connection
from happybase.table import Table

from byn.utils import retryable


logger = logging.getLogger(__name__)


FAILABLE_CONNECTION_METHODS = 'tables', 'create_table',  'enable_table', 'disable_table', 'is_table_enabled', 'compact_table'
FAILABLE_TABLE_METHODS = 'counter_inc', 'delete', 'put', 'scan', 'cells', 'row', 'rows', 'families'


class RetryableConnection(Connection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name in FAILABLE_CONNECTION_METHODS:
            setattr(
                self,
                field_name,
                retryable(
                    getattr(super(RetryableConnection, self), field_name),
                    expected_exception=Exception,
                    callback=self.reset_client
                )
            )

    def reset_client(self, *args, **kwargs):
        logger.info('Refreshng the connection %s', self)
        self._refresh_thrift_client()
        self.open()
        logger.info('Connection %s is refreshed', self)


class RetryableTable(Table):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name in FAILABLE_TABLE_METHODS:
            original_method = getattr(super(RetryableTable, self), field_name)

            setattr(
                self,
                field_name,
                retryable(
                    original_method,
                    expected_exception=Exception,
                    callback=self.connection.reset_client
                )
            )


def monkeypatch_happybase():
    happybase.connection.Connection = RetryableConnection
    happybase.pool.Connection = RetryableConnection
    happybase.connection.Table = RetryableTable
    logger.info('happybase is monkeypatched!')