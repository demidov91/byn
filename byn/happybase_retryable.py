import logging

import happybase.connection
import happybase.pool
import happybase.table
from happybase.connection import Connection
from happybase.table import Table
from happybase.batch import Batch

from byn.retry import retryable, retryable_generator


logger = logging.getLogger(__name__)


class RetryableMixin:
    FAILABLE_SYNC_METHODS = ()
    FAILABLE_GENERATOR_METHODS = ()

    def _retry_callback(self, *args, **kwargs):
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for field_name in self.FAILABLE_SYNC_METHODS:
            setattr(
                self,
                field_name,
                retryable(
                    getattr(super(), field_name),
                    expected_exception=Exception,
                    callback=self._retry_callback
                )
            )

        for field_name in self.FAILABLE_GENERATOR_METHODS:
            setattr(
                self,
                field_name,
                retryable_generator(
                    getattr(super(), field_name),
                    expected_exception=Exception,
                    callback=self._retry_callback
                )
            )


class RetryableConnection(RetryableMixin, Connection):
    FAILABLE_SYNC_METHODS = (
        'tables', 'create_table',
        'enable_table', 'disable_table',
        'is_table_enabled', 'compact_table'
    )

    def _retry_callback(self, *args, **kwargs):
        self.reset_client()

    def reset_client(self, *args, **kwargs):
        logger.info('Refreshng the connection %s', self)
        self._refresh_thrift_client()
        self.open()
        logger.info('Connection %s is refreshed', self)


class RetryableTable(RetryableMixin, Table):
    def _retry_callback(self, *args, **kwargs):
        self.connection.reset_client()

    FAILABLE_SYNC_METHODS = 'counter_inc', 'cells', 'row', 'rows', 'families'
    FAILABLE_GENERATOR_METHODS = 'scan',



class RetryableBatch(RetryableMixin, Batch):
    FAILABLE_SYNC_METHODS = 'send',

    def _retry_callback(self, *args, **kwargs):
        self._table.connection.reset_client()


def monkeypatch_happybase():
    happybase.connection.Connection = RetryableConnection
    happybase.pool.Connection = RetryableConnection
    happybase.connection.Table = RetryableTable
    happybase.table.Batch = RetryableBatch
    logger.info('happybase is monkeypatched!')
