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
import luigi.date_interval
import datetime
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, PathSelectionByDateIntervalTask

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
    collection = None

    def requires(self):
        yield self.credential_task()

    def credential_task(self):
        return ExternalURL(url=self.credentials)

    def init_env(self):
        # init conn
        credentials_target = self.credential_task().output()
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
        self.collection = connection.db[self.database]

    def check_mongo_availability(self):
        if not self.connection.connected:
            raise ImportError('mongo client not available')


class LoadRawEventFromMongoTask(MongoTask):
    event_filter = luigi.Parameter()

    def load_data(self):
        log.info('LoadRawEventFromMongoTask load_data running')
        log.info('event_filter = {}'.format(self.event_filter))
        return self.collection.find(self.event_filter)


class LoadEventFromLogFileNoExpandIntervalTask(UniversalDataTask, EventLogSelectionDownstreamMixin):
    def requires(self):
        return PathSelectionByDateIntervalTask(
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            date_pattern=self.date_pattern,
            expand_interval=datetime.timedelta(0)
        )

    def load_data(self):
        for log_file in luigi.task.flatten(self.input()):
            with log_file.open('r') as temp_file:
                with gzip.GzipFile(fileobj=temp_file) as input_file:
                    log.info('reading log file={}'.format(input_file))
                    events = self.get_raw_events_from_log_file(input_file)
                    if not events:
                        continue
                    yield events

    def get_raw_events_from_log_file(self, input_file):
        # override parent class to disable event filter
        raw_events = []
        for line in input_file:

            event_row = eventlog.parse_json_event(line)
            if not event_row:
                continue
            timestamp = eventlog.get_event_time(event_row)
            if not timestamp:
                continue
            event_row['timestamp'] = timestamp
            raw_events.append(event_row)
        return raw_events


@workflow_entry_point
class LoadEventToMongoTask(MongoTask):
    interval = luigi.DateIntervalParameter(default=None)

    def load_data(self):
        return self.log_file_selection_task().output()

    def processing(self, events_gen):
        for events in events_gen:
            self.collection.insert_many(events)
            log.info('insert {} documents to mongo'.format(len(events)))
        return events_gen

    def log_file_selection_task(self):
        if not self.interval:
            log.info('not spec interval, load yesterday log file.')
            current = datetime.datetime.utcnow().date()
            yesterday = (current - datetime.timedelta(days=1)).isoformat()
            self.interval = luigi.DateIntervalParameter().parse('{}-{}'.format(yesterday, yesterday))
        log.info('load {} log file'.format(self.interval))
        return LoadEventFromLogFileNoExpandIntervalTask(interval=self.interval)

    def requires(self):
        for req in super(LoadEventToMongoTask, self).requires():
            yield req
        yield self.log_file_selection_task()
    #
    # def complete(self):
    #     return self.output().exist()
    #
    # def output(self):
    #     return MongoTarget(interval=self.interval)
