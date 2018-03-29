import gzip

from datetime import datetime
import luigi
import logging
import traceback

from edx.analytics.tasks.common.mysql_load import get_mysql_query_results

from edx.analytics.tasks.insights.database_imports import DatabaseImportMixin
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util.url import ExternalURL
from edx.analytics.tasks.util import eventlog, opaque_key_util

log = logging.getLogger(__name__)


class UniversalDataTask(luigi.Task):
    result = []
    completed = False

    def output(self):
        return self.result

    def complete(self):
        if isinstance(self.result, list):
            return len(self.result) > 0 or self.completed
        else:
            # generator
            return self.completed

    def run(self):
        self.init_env()
        data = self.load_data()
        length = 0
        if data:
            if isinstance(data, list):
                length = len(data)
                log.info('load {} data succ'.format(length))
            else:
                log.info('load data generator succ')
        else:
            log.info('load empty data')

        try:
            log.info('data processing......')
            if length > 0:
                self.result = self.processing(data)
                log.info('{} data process completed'.format(length))
            else:
                log.info('No data need to process')
            self.completed = True
            # self.output()
        except Exception, e:
            log.error('Processing data error:{}'.format(traceback.format_exc()))

    def processing(self, data):
        return data

    def load_data(self):
        return []

    def init_env(self):
        pass


class LoadDataFromDatabaseTask(DatabaseImportMixin, UniversalDataTask):

    def load_data(self):
        log.info('query_sql = [{}]'.format(self.query))
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.query)
        return query_result

    @property
    def query(self):
        """The query builder that controls the structure and fields loaded"""
        raise NotImplementedError

    def requires(self):
        yield ExternalURL(url=self.credentials)


class LoadEventTask(EventLogSelectionMixin, UniversalDataTask):
    batch_counter_default = 1

    counter_category_name = 'Default Category'

    _counter_dict = {}

    def init_env(self):
        super(LoadEventTask, self).init_env()
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def _incr_counter(self, *args):
        """ Increments a Hadoop counter

        Note that this seems to be a bit slow, ~1 ms. Don't overuse this function by updating very frequently.
        """
        if len(args) == 2:
            # backwards compatibility with existing hadoop jobs
            group_name, count = args
            # log.debug('reporter:counter:%s,%s' % (group_name, count))
        else:
            group, name, count = args
            # log.debug('reporter:counter:%s,%s,%s' % (group, name, count))

    def incr_counter(self, *args, **kwargs):
        """ Increments a Hadoop counter

        Since counters can be a bit slow to update, this batches the updates.
        """
        threshold = kwargs.get("threshold", self.batch_counter_default)
        if len(args) == 2:
            # backwards compatibility with existing hadoop jobs
            group_name, count = args
            key = (group_name,)
        else:
            group, name, count = args
            key = (group, name)

        ct = self._counter_dict.get(key, 0)
        ct += count
        if ct >= threshold:
            new_arg = list(key) + [ct]
            self._incr_counter(*new_arg)
            ct = 0
        self._counter_dict[key] = ct

    def get_event_and_date_string(self, entity):
        event = self.parse_event_from_entity(entity)
        if event is None:
            self.incr_counter('Event', 'Discard Unparseable Event', 1)
            return None

        event_time = self.get_event_time(event)
        if not event_time:
            self.incr_counter('Event', 'Discard Missing Time Field', 1)
            return None

        # Don't use strptime to parse the date, it is extremely slow
        # to do so. Instead rely on alphanumeric comparisons.  The
        # timestamp is ISO8601 formatted, so dates will look like
        # %Y-%m-%d.  For example: 2014-05-20.
        date_string = event_time.split("T")[0]

        if date_string < self.lower_bound_date_string or date_string >= self.upper_bound_date_string:
            # Slow: self.incr_counter('Event', 'Discard Outside Date Interval', 1)
            return None

        return event, date_string

    def load_data(self):
        return self.load_raw_events()

    def parse_event_from_entity(self, line):
        raise NotImplementedError

    def load_raw_events(self):
        raise NotImplementedError


class LoadEventFromLocalFileTask(LoadEventTask):

    def load_raw_events(self):
        raw_events = []
        for log_file in luigi.task.flatten(self.input()):
            with log_file.open('r') as temp_file:
                with gzip.GzipFile(fileobj=temp_file) as input_file:
                    log.info('reading log file={}'.format(input_file))
                    events = self.get_raw_events_from_log_file(input_file)
                    if not events:
                        continue
                    raw_events.extend(events)
        return raw_events

    def get_raw_events_from_log_file(self, input_file):
        raw_events = []
        for line in input_file:
            event_row = self.get_event_row_from_line(line)
            if not event_row:
                continue
            raw_events.append(event_row)
        return raw_events

    def parse_event_from_entity(self, line):
        return eventlog.parse_json_event(line)

    def get_event_row_from_line(self, line):
        return NotImplementedError


class LoadEventFromMongoTask(LoadEventTask):
    lower_bound_date_timestamp = None
    upper_bound_date_timestamp = None

    def init_env(self):
        super(LoadEventFromMongoTask, self).init_env()
        date_a = self.interval.date_a.toordinal()
        date_b = self.interval.date_b.toordinal()
        self.lower_bound_date_timestamp = datetime.fromordinal(date_a)
        self.upper_bound_date_timestamp = datetime.fromordinal(date_b)

    def parse_event_from_entity(self, document):
        return document

    def load_raw_events(self):
        event_iter = self.input()
        return event_iter

    def requires(self):
        yield self.mongo_load_task()

    def mongo_load_task(self):
        from edx.analytics.tasks.common.mongo import LoadRawEventFromMongoTask
        return LoadRawEventFromMongoTask(filter=self.event_filter())

    def processing(self, event_iter):
        raw_event = []
        for event in event_iter:
            row = self.get_event_row_from_document(event)
            if row:
                raw_event.append(row)
        return raw_event

    def event_filter(self):
        # return {
        #     '$and': [
        #         {'timestamp': {'$lte': self.upper_bound_date_timestamp}},
        #         {'timestamp': {'$gte': self.lower_bound_date_timestamp}}
        #     ]}
        raise NotImplementedError

    def get_event_row_from_document(self, document):
        raise NotImplementedError
