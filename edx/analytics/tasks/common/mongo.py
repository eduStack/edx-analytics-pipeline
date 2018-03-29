"""
Support for loading data into a Mysql database.
"""
import gzip
import json
import logging
import traceback

import os
import pymongo
import luigi.configuration
from edx.analytics.tasks.util.decorators import workflow_entry_point

from edx.analytics.tasks.util import eventlog

from edx.analytics.tasks.util.data import UniversalDataTask, LoadEventFromLocalFileTask
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)


class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kwargs)
        return cls._instance


class MongoConn(Singleton):

    def __init__(self, username, password, db, host='localhost', port=27017):
        # connect db
        try:
            self.conn = pymongo.MongoClient(host, port)
            self.db = self.conn[db]  # connect db
            self.username = username
            self.password = password
            if self.username and self.password:
                self.connected = self.db.authenticate(self.username, self.password)
            else:
                self.connected = True
        except Exception:
            log.error('Connect to Mongo error: {}'.format(traceback.format_exc()))


class MongoTaskMixin(object):
    """
    Parameters for inserting a data set into RDBMS.

    """
    database = luigi.Parameter(
        default='insights',
        config_path={'section': 'mongo', 'name': 'database'},
        description='The name of the database to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'mongo', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    insert_chunk_size = luigi.IntParameter(
        default=100,
        significant=False,
        description='The number of rows to insert at a time.',
    )


class MongoTask(MongoTaskMixin, UniversalDataTask):
    connection = None
    db = None

    def requires(self):
        yield ExternalURL(url=self.credentials)

    def init_env(self):
        # init conn
        credentials_target = self.input()
        cred = None
        with credentials_target.open('r') as credentials_file:
            cred = json.load(credentials_file)

        connection = MongoConn(username=cred.get('username'),
                               password=cred.get('password'),
                               db=self.database,
                               host=cred.get('host'),
                               port=cred.get('port'))
        self.connection = connection
        # check
        self.check_mongo_availability()
        self.db = connection.db[self.database]

    def check_mongo_availability(self):
        if not self.connection.connected:
            raise ImportError('mongo client not available')


class LoadRawEventFromMongoTask(MongoTask):
    event_filter = luigi.Parameter(significant=False)

    def load_data(self):
        log.info('LoadRawEventFromMongoTask load_data running')
        log.info('event_filter = {}'.format(self.event_filter))
        return self.db.find(self.event_filter)


class LogFileImportMixin(object):
    log_path = luigi.Parameter(
        default='/tmp/tracking',
        config_path={'section': 'mongo', 'name': 'log_path'},
        description='Path to log file imported to mongo.'
    )
    processed_path = luigi.Parameter(
        default='/tmp/processed',
        config_path={'section': 'mongo', 'name': 'processed_path'}
    )


class LoadEventFromLogFileWithoutIntervalTask(LogFileImportMixin, LoadEventFromLocalFileTask):
    # TODO make sure log_path and processed_path exist

    def requires(self):
        # do not invoke parent class method
        yield [ExternalURL(url) for url in self.get_log_file_paths()]

    def get_raw_events_from_log_file(self, input_file):
        # override parent class to disable event filter
        raw_events = []
        for line in input_file:
            event_row = eventlog.parse_json_event(line)
            if not event_row:
                continue
            raw_events.append(event_row)
        return raw_events

    def output(self):
        # before output we need remove processed files
        for log_file in luigi.task.flatten(self.input()):
            log_file.move(self.processed_path)
        return super(LoadEventFromLogFileWithoutIntervalTask, self).output()

    def get_log_file_paths(self):
        for source in self.log_path:
            for directory_path, _subdir_paths, filenames in os.walk(source):
                for filename in filenames:
                    yield os.path.join(directory_path, filename)

@workflow_entry_point
class LoadEventToMongoTask(MongoTask):

    def load_data(self):
        return self.log_file_selection_task().output()

    def processing(self, data):
        self.db.insert_many(data)
        return data

    def log_file_selection_task(self):
        return LoadEventFromLogFileWithoutIntervalTask()

    def requires(self):
        for req in super(LoadEventToMongoTask, self).requires():
            yield req
        yield self.log_file_selection_task()
