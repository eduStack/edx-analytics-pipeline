"""
Support for loading data into a Mysql database.
"""
import json
import logging
import traceback

import pymongo
import luigi.configuration
from edx.analytics.tasks.util.data import UniversalDataTask
from edx.analytics.tasks.util.url import ExternalURL

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


class MongoLoadTaskMixin(object):
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
    log_path = luigi.Parameter(
        default='/tmp/tracking',
        config_path={'section': 'mongo', 'name': 'log_path'},
        description='Path to log file imported to mongo.'
    )


class LoadRawEventFromMongoTask(MongoLoadTaskMixin, UniversalDataTask):
    event_filter = luigi.Parameter(significant=False)
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

    def load_data(self):
        log.info('LoadRawEventFromMongoTask load_data running')
        log.info('event_filter = {}'.format(self.event_filter))
        return self.db.find(self.event_filter)

    # @property
    # def indexes(self):
    #     """List of tuples defining the names of the columns to include in each index."""
    #     return []
    #
    # @property
    # def keys(self):
    #     """List of tuples defining other keys to include in the table definition."""
    #     return []

    def check_mongo_availability(self):
        if not self.connection.connected:
            raise ImportError('mongo client not available')
