"""
Determine the number of users in each country are enrolled in each course.
"""
import datetime
import gzip
import logging
import textwrap
from collections import defaultdict
import tempfile

import luigi
import pandas as pd
from luigi.hive import HiveQueryTask
from edx.analytics.tasks.common.sqoop import SqoopImportMixin
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask, get_mysql_query_results
from edx.analytics.tasks.common.pathutil import (
    EventLogSelectionDownstreamMixin, EventLogSelectionMixin, PathSelectionByDateIntervalTask
)
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.geolocation import GeolocationDownstreamMixin, GeolocationMixin
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin, hive_database_name
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import ExternalURL, UncheckedExternalURL, get_target_from_url, url_path_join

try:
    import pygeoip
except ImportError:
    # The module will be imported on slave nodes even though they don't actually have the package installed.
    # The module is hopefully exported for tasks that actually use the module.
    pygeoip = NotImplemented

log = logging.getLogger(__name__)


class LastIpAddressRecord(Record):
    """
    Store information about last IP address observed for a given user in a given course.

    Values are not written to a database, so string lengths are not specified.
    """
    timestamp = StringField(description='Timestamp of last event by user in a course.')
    ip_address = StringField(description='IP address recorded on last event by user in a course.')
    username = StringField(description='Username recorded on last event by user in a course.')
    course_id = StringField(description='Course ID recorded on last event by user in a course.')


class LastCountryOfUserDownstreamMixin(OverwriteOutputMixin,
                                       EventLogSelectionDownstreamMixin,
                                       GeolocationDownstreamMixin):
    """
    Defines parameters for LastCountryOfUser task and downstream tasks that require it.

    """

    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to extract ip addresses for. '
                    'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        config_path={'section': 'location-per-course', 'name': 'interval_start'},
        significant=False,
        description='The start date to extract ip addresses for.  Ignored if `interval` is provided.',
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='The end date to extract ip addresses for.  Ignored if `interval` is provided. '
                    'Default is today, UTC.',
    )

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'location-per-course', 'name': 'overwrite_n_days'},
        significant=False,
        description='This parameter is used by LastCountryOfUser which will overwrite ip address per user'
                    ' for the most recent n days.'
    )

    def __init__(self, *args, **kwargs):
        super(LastCountryOfUserDownstreamMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class LastCountryOfUserRecord(Record):
    """For a given username, stores information about last country."""
    country_name = StringField(length=255, description="Name of last country.")
    country_code = StringField(length=10, description="Code for last country.")
    username = StringField(length=255, description="Username of user with country information.")


class LastCountryOfUserEventLogSelectionTask(LastCountryOfUserDownstreamMixin, EventLogSelectionMixin, luigi.Task):
    completed = False
    batch_counter_default = 1
    _counter_dict = {}

    counter_category_name = 'LastCountryOfUser Events'

    # FILEPATH_PATTERN should match the output files defined by output_path_for_key().
    FILEPATH_PATTERN = '.*?last_ip_of_user_(?P<date>\\d{4}-\\d{2}-\\d{2})'

    def __init__(self, *args, **kwargs):
        super(LastCountryOfUserEventLogSelectionTask, self).__init__(*args, **kwargs)

        self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)

    def complete(self):
        return self.completed
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def get_raw_events_from_log_file(self, input_file):
        raw_events = []
        for line in input_file:
            value = self.get_event_and_date_string(line)
            if value is None:
                continue
            event, date_string = value

            username = eventlog.get_event_username(event)
            if not username:
                continue

            # Get timestamp instead of date string, so we get the latest ip
            # address for events on the same day.
            timestamp = eventlog.get_event_time_string(event)
            if not timestamp:
                continue

            ip_address = event.get('ip')
            if not ip_address:
                log.warning("No ip_address found for user '%s' on '%s'.", username, timestamp)
                continue

            # Get the course_id from context, if it happens to be present.
            # It's okay if it isn't.

            # (Not sure if there are particular types of course
            # interaction we care about, but we might want to only collect
            # the course_id off of explicit events, and ignore implicit
            # events as not being "real" interactions with course content.
            # Or maybe we add a flag indicating explicit vs. implicit, so
            # that this can be better teased apart.  For example, we could
            # use the latest explicit event for a course, but if there are
            # none, then use the latest implicit event for the course, and
            # if there are none, then use the latest overall event.)
            course_id = eventlog.get_course_id(event)

            # For multi-output, we will generate a single file for each key value.
            # When looking at location for user in a course, we don't want to have
            # an output file per course per date, so just use date as the key,
            # and have a single file representing all events on the date.
            event_row = (date_string, timestamp, ip_address, course_id, username)

            raw_events.append(event_row)
        return raw_events

    def output(self):
        raw_events = []
        for log_file in luigi.task.flatten(self.input()):
            with log_file.open('r') as temp_file:
                with gzip.GzipFile(fileobj=temp_file) as input_file:
                    log.info('reading log file={}'.format(input_file))
                    events = self.get_raw_events_from_log_file(input_file)
                    if not events:
                        continue
                    raw_events.extend(events)
        columns = ['date_string', 'timestamp', ' ip_address', 'course_id', ' username']
        # log.info('raw_events = {}'.format(raw_events))
        df = pd.DataFrame(data=raw_events, columns=columns)
        for date_string, group in df.groupby(['date_string']):
            values = group.get_values()
            last_ip = defaultdict()
            last_timestamp = defaultdict()
            batch_values = []
            for value in values:
                (_date_string, timestamp, ip_address, course_id, username) = value

                # We are storing different IP addresses depending on the username
                # *and* the course.  This anticipates a future requirement to provide
                # different countries depending on which course.
                last_key = (username, course_id)

                last_time = last_timestamp.get(last_key, '')
                if timestamp > last_time:
                    last_ip[last_key] = ip_address
                    last_timestamp[last_key] = timestamp

            # Now output the resulting "last" values for each key.
            for last_key, ip_address in last_ip.iteritems():
                timestamp = last_timestamp[last_key]
                username, course_id = last_key
                # value = [timestamp, ip_address, username, course_id]
                batch_values.append((username, timestamp, ip_address))
            yield batch_values

    def init_local(self):
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def run(self):
        self.init_local()
        log.info('LastCountryOfUserEventLogSelectionTask running')
        super(LastCountryOfUserEventLogSelectionTask, self).run()
        if not self.completed:
            self.completed = True

    def requires(self):
        requires = super(LastCountryOfUserEventLogSelectionTask, self).requires()
        if isinstance(requires, luigi.Task):
            yield requires

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


class LastCountryOfUserDataTask(LastCountryOfUserDownstreamMixin, GeolocationMixin, luigi.Task):
    """
    Identifies the country of the last IP address associated with each user.

    Uses :py:class:`LastCountryOfUserDownstreamMixin` to define parameters, :py:class:`EventLogSelectionMixin`
    to define required input log files, and :py:class:`GeolocationMixin` to provide geolocation setup.

    """
    completed = False

    def complete(self):
        return self.completed
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def init_local(self):
        self.temporary_data_file = tempfile.NamedTemporaryFile(prefix='geolocation_data')
        with self.geolocation_data_target().output().open() as geolocation_data_input:
            while True:
                transfer_buffer = geolocation_data_input.read(1024)
                if transfer_buffer:
                    self.temporary_data_file.write(transfer_buffer)
                else:
                    break
        self.temporary_data_file.seek(0)
        self.geoip = pygeoip.GeoIP(self.temporary_data_file.name, pygeoip.STANDARD)
        log.info('geo data init succ!')

    def requires_local(self):
        return LastCountryOfUserEventLogSelectionTask(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )

    def output(self):
        raw_recoreds = self.requires_local().output()
        recoreds = []
        for recored in raw_recoreds:
            recoreds.extend(recored)
        columns = ['username', 'timestamp', 'ip_address']

        df = pd.DataFrame(data=recoreds, columns=columns)

        for username, group in df.groupby(['username']):
            values = group[['timestamp', 'ip_address']].get_values()
            last_ip = None
            last_timestamp = ""
            for timestamp, ip_address in values:
                if timestamp > last_timestamp:
                    last_ip = ip_address
                    last_timestamp = timestamp

            if not last_ip:
                continue

            debug_message = u"user '{}' on '{}'".format(username, last_timestamp)
            country = self.get_country_name(last_ip, debug_message)
            code = self.get_country_code(last_ip, debug_message)

            # Add the username for debugging purposes.  (Not needed for counts.)
            yield country.encode('utf8'), code.encode('utf8'), username.encode('utf8')

    def run(self):
        self.init_local()
        log.info('LastCountryOfUser running')
        super(LastCountryOfUserDataTask, self).run()
        if not self.completed:
            self.completed = True
        # self.final_reducer()


    def geolocation_data_target(self):
        return ExternalURL(self.geolocation_data)

    def requires(self):
        requires = super(LastCountryOfUserDataTask, self).requires()
        if isinstance(requires, luigi.Task):
            yield requires
        yield self.geolocation_data_target()
        yield self.requires_local()


class LastCountryOfUser(LastCountryOfUserDownstreamMixin, MysqlInsertTask):
    """
    Copy the last_country_of_user table from Map-Reduce into MySQL.
    """

    @property
    def table(self):
        return "last_country_of_user"

    @property
    def columns(self):
        return LastCountryOfUserRecord.get_sql_schema()

    @property
    def insert_source_task(self):
        return None

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    def requires_local(self):
        return LastCountryOfUserDataTask(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )

    def requires(self):
        yield super(LastCountryOfUser, self).requires()['credentials']
        yield self.requires_local()


class InsertToMysqlLastCountryOfUserTask(LastCountryOfUserDownstreamMixin, MysqlInsertTask):
    """
    Copy the last_country_of_user table from Map-Reduce into MySQL.
    """

    @property
    def table(self):
        return "last_country_of_user"

    @property
    def columns(self):
        return LastCountryOfUserRecord.get_sql_schema()

    @property
    def insert_source_task(self):
        return None

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    def requires_local(self):
        return LastCountryOfUser(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )

    def requires(self):
        yield super(InsertToMysqlLastCountryOfUserTask, self).requires()['credentials']
        yield self.requires_local()


class LastCountryPerCourseRecord(Record):
    """For a given course, stores aggregates about last country."""
    date = DateField(nullable=False, description="Date of course enrollment data.")
    course_id = StringField(length=255, nullable=False,
                            description="Course ID for course/last-country pair being counted.")
    country_code = StringField(length=10, nullable=True,
                               description="Code for country in course/last-country pair being counted.")
    count = IntegerField(nullable=False,
                         description="Number enrolled in course whose current last country code matches.")
    cumulative_count = IntegerField(nullable=False,
                                    description="Number ever enrolled in course whose current last-country code matches.")


class AuthUserSelectionTask(SqoopImportMixin, luigi.Task):
    completed = False

    def complete(self):
        return self.completed
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        query = """
                SELECT 
                    username,
                    last_login,
                    date_joined,
                    is_active,
                    is_superuser,
                    is_staff,
                    email
                FROM auth_user
            """
        return query

    def output(self):
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        log.info('query_sql = [{}]'.format(self.insert_query))
        for row in query_result:
            yield row

    def run(self):
        log.info('AuthUserSelectionTask running')
        if not self.completed:
            self.completed = True

    def requires(self):
        yield ExternalURL(url=self.credentials)


class ImportAuthUserTask(MysqlInsertTask):

    def __init__(self, *args, **kwargs):
        super(ImportAuthUserTask, self).__init__(*args, **kwargs)

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def table(self):  # pragma: no cover
        return 'auth_user'

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    @property
    def columns(self):
        return [
            ('username', 'VARCHAR(255)'),
            ('last_login', 'TIMESTAMP'),
            ('date_joined', 'TIMESTAMP'),
            ('is_active', 'TINYINT(1)'),
            ('is_superuser', 'TINYINT(1)'),
            ('is_staff', 'TINYINT(1)'),
            ('email', 'VARCHAR(255)'),
        ]

    @property
    def indexes(self):
        return [
            ('username',),
        ]

    def requires_local(self):
        return AuthUserSelectionTask()

    def requires(self):
        yield super(ImportAuthUserTask, self).requires()['credentials']

        requires_local = self.requires_local()
        if isinstance(requires_local, luigi.Task):
            yield requires_local


class StudentCourseEnrollmentSelectionTask(SqoopImportMixin, luigi.Task):
    completed = False

    def complete(self):
        return self.completed
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        query = """
                SELECT 
                    user_id,
                    course_id,
                    date(created) dt,
                    is_active,
                    `mode`
                FROM student_courseenrollment
            """
        return query

    def output(self):
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        log.info('query_sql = [{}]'.format(self.insert_query))
        for row in query_result:
            yield row

    def run(self):
        log.info('StudentCourseEnrollmentSelectionTask running')
        if not self.completed:
            self.completed = True

    def requires(self):
        yield ExternalURL(url=self.credentials)


class ImportStudentCourseEnrollmentTask(MysqlInsertTask):

    def __init__(self, *args, **kwargs):
        super(ImportStudentCourseEnrollmentTask, self).__init__(*args, **kwargs)

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def table(self):  # pragma: no cover
        return 'student_courseenrollment'

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'VARCHAR(255)'),
            ('dt', 'TIMESTAMP'),
            ('is_active', 'TINYINT(1)'),
            ('mode', 'VARCHAR(255)'),
        ]

    @property
    def indexes(self):
        return [
            ('user_id',),
        ]

    def requires_local(self):
        return StudentCourseEnrollmentSelectionTask()

    def requires(self):
        yield super(ImportStudentCourseEnrollmentTask, self).requires()['credentials']

        requires_local = self.requires_local()
        if isinstance(requires_local, luigi.Task):
            yield requires_local


class InsertToMysqlLastCountryPerCourseTask(LastCountryOfUserDownstreamMixin,
                                            MysqlInsertTask):  # pylint: disable=abstract-method
    """
    Define course_enrollment_location_current table.
    """

    @property
    def table(self):
        return "course_enrollment_location_current"

    @property
    def columns(self):
        return LastCountryPerCourseRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
            # Note that the order here is extremely important. The API query pattern needs to filter first by course and
            # then by date.
            ('course_id', 'date'),
        ]

    @property
    def insert_source_task(self):
        return None

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        query = """
            SELECT
                sce.dt,
                sce.course_id,
                uc.country_code,
                sum(if(sce.is_active, 1, 0)),
                count(sce.user_id)
            FROM student_courseenrollment sce
            LEFT OUTER JOIN auth_user au on sce.user_id = au.id
            LEFT OUTER JOIN last_country_of_user uc on au.username = uc.username
            GROUP BY sce.dt, sce.course_id, uc.country_code;
        """
        return query

    def rows(self):
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        log.info('query_sql = [{}]'.format(self.insert_query))
        for row in query_result:
            yield row

    def requires(self):
        yield super(InsertToMysqlLastCountryPerCourseTask, self).requires()['credentials']
        yield ImportStudentCourseEnrollmentTask()
        yield ImportAuthUserTask()
        yield LastCountryOfUser(
            source=self.source,
            pattern=self.pattern,
            interval=self.interval,
            interval_start=self.interval_start,
            interval_end=self.interval_end,
            overwrite_n_days=self.overwrite_n_days,
            geolocation_data=self.geolocation_data,
            overwrite=self.overwrite,
        )


@workflow_entry_point
class HylInsertToMysqlCourseEnrollByCountryWorkflow(
    LastCountryOfUserDownstreamMixin,
    luigi.WrapperTask):
    """
    Write last-country information to Mysql.

    Includes LastCountryOfUser and LastCountryPerCourse.
    """

    # Because this has OverwriteOutputMixin and WrapperTask, we have to redefine complete() to
    # work correctly.
    def run(self):
        if self.overwrite:
            self.attempted_removal = True
        super(HylInsertToMysqlCourseEnrollByCountryWorkflow, self).run()

    def requires(self):
        kwargs = {
            'source': self.source,
            'pattern': self.pattern,
            'interval': self.interval,
            'interval_start': self.interval_start,
            'interval_end': self.interval_end,
            'overwrite_n_days': self.overwrite_n_days,
            'geolocation_data': self.geolocation_data,
            'overwrite': self.overwrite,
        }

        yield (
            # InsertToMysqlLastCountryOfUserTask(**kwargs),
            InsertToMysqlLastCountryPerCourseTask(**kwargs),
        )
