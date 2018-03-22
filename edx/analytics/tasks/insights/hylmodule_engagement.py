"""
Measure student engagement with individual modules in the course.

See ModuleEngagementWorkflowTask for more extensive documentation.
"""

import datetime
import gzip
import logging
import random
from collections import defaultdict

import luigi.task
import pandas as pd
from luigi import date_interval

from edx.analytics.tasks.common.elasticsearch_load import ElasticsearchIndexTask
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.mysql_load import IncrementalMysqlTableInsertTask, MysqlTableTask, \
    get_mysql_query_results
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.insights.database_imports import (
    DatabaseImportMixin
)
from edx.analytics.tasks.insights.hylenrollments import ImportAuthUserProfileTask
from edx.analytics.tasks.insights.hyllocation_per_course import ImportAuthUserTask
from edx.analytics.tasks.insights.enrollments import ExternalCourseEnrollmentPartitionTask
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin, hive_database_name
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, FloatField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

try:
    import numpy
except ImportError:
    numpy = None  # pylint: disable=invalid-name

log = logging.getLogger(__name__)

METRIC_RANGE_HIGH = 'high'
METRIC_RANGE_NORMAL = 'normal'
METRIC_RANGE_LOW = 'low'

SEGMENT_HIGHLY_ENGAGED = 'highly_engaged'
SEGMENT_STRUGGLING = 'struggling'


class OverwriteFromDateMixin(object):
    """Supports overwriting a subset of the data to compensate for late events."""

    overwrite_from_date = luigi.DateParameter(
        description='This parameter is passed down to the module engagement model which will overwrite data from a date'
                    ' in the past up to the end of the interval. Events are not always collected at the time that they'
                    ' are emitted, sometimes much later. By re-processing past days we can gather late events and'
                    ' include them in the computations.',
        default=None,
        significant=False
    )


class ModuleEngagementDownstreamMixin(EventLogSelectionDownstreamMixin, OverwriteFromDateMixin):
    """Common parameters and base classes used to pass parameters through the engagement workflow."""

    # Required parameter
    date = luigi.DateParameter(
        description='Upper bound date for the end of the interval to analyze. Data produced before 00:00 on this'
                    ' date will be analyzed. This workflow is intended to run nightly and this parameter is intended'
                    ' to be set to "today\'s" date, so that all of yesterday\'s data is included and none of today\'s.'
    )

    # Override superclass to disable this parameter
    interval = None


class ModuleEngagementRosterIndexDownstreamMixin(object):
    """Indexing parameters that can be specified at the workflow level."""

    obfuscate = luigi.BooleanParameter(
        default=False,
        description='Generate fake names and email addresses for users. This can be used to generate production-like'
                    ' data sets that are more difficult to associate with particular users at a glance. Useful for'
                    ' testing.'
    )
    scale_factor = luigi.IntParameter(
        default=1,
        description='For each record, generate N more identical records with different IDs. This will result in a'
                    ' scaled up data set that can be used for performance testing the indexing and querying systems.'
    )
    indexing_tasks = luigi.IntParameter(
        default=None,
        significant=False,
        description=ElasticsearchIndexTask.indexing_tasks.description
    )


class WeekIntervalMixin(object):
    """
    For tasks that accept a date parameter that represents the end date of a week.

    The date is used to set an `interval` attribute that represents the complete week.
    """

    def __init__(self, *args, **kwargs):
        super(WeekIntervalMixin, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)


class ModuleEngagementRecord(Record):
    """Represents a count of interactions performed by a user on a particular entity (usually a module in a course)."""

    course_id = StringField(length=255, nullable=False, description='Course the learner interacted with.')
    username = StringField(length=30, nullable=False, description='Learner\'s username.')
    date = DateField(nullable=False, description='The learner interacted with the entity on this date.')
    entity_type = StringField(length=10, nullable=False, description='Category of entity that the learner interacted'
                                                                     ' with. Example: "video".')
    entity_id = StringField(length=255, nullable=False, truncate=True,
                            description='A unique identifier for the entity within the'
                                        ' course that the learner interacted with.')
    event = StringField(length=30, nullable=False, description='The interaction the learner had with the entity.'
                                                               ' Example: "viewed".')
    count = IntegerField(nullable=False, description='Number of interactions the learner had with this entity on this'
                                                     ' date.')


class ModuleEngagementRosterRecord(Record):
    """A summary of statistics related to a single learner in a single course related to their engagement."""
    course_id = StringField(length=255, nullable=False, description='Course the learner is enrolled in.')
    username = StringField(length=255, nullable=False, description='Learner\'s username.')
    start_date = DateField(description='Analysis includes all data from 00:00 on this day up to the end date.')
    end_date = DateField(description='Analysis includes all data up to but not including this date.')
    email = StringField(length=255, nullable=False, description='Learner\'s email address.')
    name = StringField(length=255, nullable=False, analyzed=True,
                       description='Learner\'s full name including first, middle and last names. '
                                   'This field can be searched by instructors.')
    enrollment_mode = StringField(length=255, nullable=False,
                                  description='Learner is enrolled in the course with this mode. Example: verified.')
    enrollment_date = DateField(description='First date the learner enrolled in the course.')
    cohort = StringField(length=255, nullable=False, description='Cohort the learner belongs to, can be null.')
    problem_attempts = IntegerField(description='Number of times the learner attempted any problem in the course.')
    problems_attempted = IntegerField(description='Number of unique problems the learner has ever attempted in the'
                                                  ' course.')
    problems_completed = IntegerField(description='Number of unique problems the learner has ever completed correctly'
                                                  ' in the course.')
    problem_attempts_per_completed = FloatField(description='Ratio of the number of attempts the learner has made on'
                                                            ' any problem to the number of unique problems they have'
                                                            ' completed correctly in the course.')
    videos_viewed = IntegerField(description='Number of unique videos the learner has watched any part of in the'
                                             ' course.')
    discussion_contributions = IntegerField(description='Total number of posts, responses and comments the learner has'
                                                        ' made in the course.')
    segments = StringField(analyzed=True, description='Classifiers that help group learners by analyzing their activity'
                                                      ' patterns. Example: "inactive" indicates the user has not'
                                                      ' engaged with the course recently. This field is analyzed by'
                                                      ' elasticsearch so that searches can be made for learners that'
                                                      ' either have or don\'t have particular classifiers.')
    attempt_ratio_order = IntegerField(
        description='Used to sort learners by problem_attempts_per_completed in a meaningful way. When using'
                    ' problem_attempts_per_completed as your primary sort key, you can secondary sort by'
                    ' attempt_ratio_order to see struggling and high performing users. At one extreme this identifies'
                    ' users who have gotten many problems correct with the fewest number of attempts, at the other'
                    ' extreme it highlights users who have gotten very few (if any) problems correct with a very high'
                    ' number of attempts. The two extremes identify the highest performing and lowest performing'
                    ' learners according to this metric. To see high performing learners sort by'
                    ' (problem_attempts_per_completed ASC, attempt_ratio_order DESC). To see struggling learners sort'
                    ' by (problem_attempts_per_completed DESC, attempt_ratio_order ASC).'
    )
    # More user profile fields, appended after initial schema creation
    user_id = IntegerField(description='Learner\'s user ID.')
    language = StringField(length=255, nullable=False, description='Learner\'s preferred language.')
    location = StringField(length=255, nullable=False, description='Learner\'s reported location.')
    year_of_birth = IntegerField(description='Learner\'s reported year of birth.')
    level_of_education = StringField(length=255, nullable=False, description='Learner\'s reported level of education.')
    gender = StringField(length=255, nullable=False, description='Learner\'s reported gender.')
    mailing_address = StringField(length=255, nullable=False, description='Learner\'s reported mailing address.')
    city = StringField(length=255, nullable=False, description='Learner\'s reported city.')
    country = StringField(length=255, nullable=False, description='Learner\'s reported country.')
    goals = StringField(length=255, nullable=False, description='Learner\'s reported goals.')


class ModuleEngagementSummaryMetricRangeRecord(Record):
    """
    Metrics are analyzed to determine interesting ranges.

    This could really be any arbitrary range, maybe just the 3rd percentile, or perhaps it could define two groups,
    below and above the median.
    """

    course_id = StringField(length=255, nullable=False, description='Course the learner interacted with.')
    start_date = DateField(description='Analysis includes all data from 00:00 up to the end date.')
    end_date = DateField(description='Analysis includes all data up to, but not including this date.')
    metric = StringField(length=50, nullable=False, description='Metric that this range applies to.')
    range_type = StringField(length=50, nullable=False, description='Type of range. For example: "low"')
    low_value = FloatField(description='Low value for the range. Exact matches are included in the range.')
    high_value = FloatField(description='High value for the range. Exact matches are excluded from the range.')


class ModuleEngagementSummaryRecord(Record):
    """
    Summarizes a user's engagement with a particular course in the past week with simple counts of activity.
    """

    course_id = StringField(length=255, nullable=False, description='Course the learner interacted with.')
    username = StringField(length=255, nullable=False, description='Learner\'s username.')
    start_date = DateField(description='Analysis includes all data from 00:00 on this day up to the end date.')
    end_date = DateField(description='Analysis includes all data up to but not including this date.')
    problem_attempts = IntegerField(is_metric=True, description='Number of times the learner attempted any problem in'
                                                                ' the course.')
    problems_attempted = IntegerField(is_metric=True, description='Number of unique problems the learner has ever'
                                                                  ' attempted in the course.')
    problems_completed = IntegerField(is_metric=True, description='Number of unique problems the learner has ever'
                                                                  ' completed correctly in the course.')
    problem_attempts_per_completed = FloatField(is_metric=True, description='Ratio of the number of attempts the'
                                                                            ' learner has made on any problem to the'
                                                                            ' number of unique problems they have'
                                                                            ' completed correctly in the course.')
    videos_viewed = IntegerField(is_metric=True, description='Number of unique videos the learner has watched any part'
                                                             ' of in the course.')
    discussion_contributions = IntegerField(is_metric=True, description='Total number of posts, responses and comments'
                                                                        ' the learner has made in the course.')
    days_active = IntegerField(description='Number of days the learner performed any activity in.')

    def get_metrics(self):
        """
        A generator that returns all fields that are metrics.

        Returns: A generator of tuples whose first element is the metric name, and the second is the value of the metric
            for this particular record.
        """
        for field_name, field_obj in self.get_fields().items():
            if getattr(field_obj, 'is_metric', False):
                yield field_name, getattr(self, field_name)


class ModuleEngagementSummaryRecordBuilder(object):
    """Gather the data needed to emit a sparse weekly course engagement record"""

    def __init__(self):
        self.problem_attempts = 0
        self.problems_attempted = set()
        self.problems_completed = set()
        self.videos_viewed = set()
        self.discussion_contributions = 0
        self.days_active = set()

    def add_record(self, record):
        """
        Updates metrics based on the provided record.

        Arguments:
            record (ModuleEngagementRecord): The record to aggregate.
        """
        self.days_active.add(record.date)

        count = int(record.count)
        if record.entity_type == 'problem':
            if record.event == 'attempted':
                self.problem_attempts += count
                self.problems_attempted.add(record.entity_id)
            elif record.event == 'completed':
                self.problems_completed.add(record.entity_id)
        elif record.entity_type == 'video':
            if record.event == 'viewed':
                self.videos_viewed.add(record.entity_id)
        elif record.entity_type == 'discussion':
            self.discussion_contributions += count
        else:
            log.warn('Unrecognized entity type: %s', record.entity_type)

    def get_summary_record(self, course_id, username, interval):
        """
        Given all of the records that have been added, generate a summarizing record.

        Arguments:
            course_id (string):
            username (string):
            interval (luigi.date_interval.DateInterval):

        Returns:
            ModuleEngagementSummaryRecord: Representing the aggregated summary of all of the learner's activity.
        """
        attempts_per_completion = self.compute_attempts_per_completion(
            self.problem_attempts,
            len(self.problems_completed)
        )

        return ModuleEngagementSummaryRecord(
            course_id,
            username,
            interval.date_a,
            interval.date_b,
            self.problem_attempts,
            len(self.problems_attempted),
            len(self.problems_completed),
            attempts_per_completion,
            len(self.videos_viewed),
            self.discussion_contributions,
            len(self.days_active)
        )

    @staticmethod
    def compute_attempts_per_completion(num_problem_attempts, num_problems_completed):
        """
        The ratio of attempts per correct problem submission is an indicator of how much a student is struggling.

        If a student has not completed any problems a value of float('inf') is returned.
        """
        if num_problems_completed > 0:
            attempts_per_completion = float(num_problem_attempts) / num_problems_completed
        else:
            attempts_per_completion = float('inf')
        return attempts_per_completion


class ModuleEngagementUserSegmentRecord(Record):
    """
    Maps a user's activity in a course to various segments.
    """

    course_id = StringField(length=255, nullable=False, description='Course the learner is enrolled in.')
    username = StringField(length=255, nullable=False, description='Learner\'s username.')
    start_date = DateField(description='Analysis includes all data from 00:00 on this day up to the end date.')
    end_date = DateField(description='Analysis includes all data up to but not including this date.')
    segment = StringField(length=255, nullable=False,
                          description='A short term that includes only lower case characters and underscores that'
                                      ' indicates a group that the user belongs to. For example: highly_engaged.')
    reason = StringField(length=255, nullable=False,
                         description='A human readable description of the reason for the student being placed in this'
                                     ' segment.')


SEGMENT_HIGHLY_ENGAGED = 'highly_engaged'
SEGMENT_STRUGGLING = 'struggling'


class ModuleEngagementDataTask(EventLogSelectionMixin, OverwriteOutputMixin, luigi.Task):
    """
    Process the event log and categorize user engagement with various types of content.

    This emits one record for each type of interaction. Note that this is loosely defined. For example, for problems, it
    will emit two records if the problem is correct (one for the "attempt" interaction and one for the "correct attempt"
    interaction).

    This task is intended to be run incrementally and populate a single Hive partition. Although time consuming to
    bootstrap the table, it results in a significantly cleaner workflow. It is much clearer what the success and
    failure conditions are for a task and the management of residual data is dramatically simplified. All of that said,
    Hadoop is not designed to operate like this and it would be significantly more efficient to process a range of dates
    at once. The choice was made to stick with the cleaner workflow since the steady-state is the same for both options
    - in general we will only be processing one day of data at a time.
    """

    # Required parameters
    date = luigi.DateParameter()
    completed = False
    # Override superclass to disable this parameter
    interval = None

    # Write the output directly to the final destination and rely on the _SUCCESS file to indicate whether or not it
    # is complete. Note that this is a custom extension to luigi.
    enable_direct_output = True

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementDataTask, self).__init__(*args, **kwargs)

        self.interval = date_interval.Date.from_date(self.date)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        entity_id, entity_type, user_actions = self.get_user_actions_from_event(event_data, event_source, event_type)

        if not entity_id or not entity_type:
            return

        result = []
        for action in user_actions:
            date = DateField().deserialize_from_string(date_string)
            result.append(((course_id, username, date, entity_type, entity_id, action), 1))
        return result

    def get_user_actions_from_event(self, event_data, event_source, event_type):
        """
        All of the logic needed to categorize the event.

        Returns: A tuple containing the ID of the entity, the type of entity (video, problem etc) and a list of actions
            that the event represents. A single event may represent several categories of action. A submitted correct
            problem, for example, will return both an attempt action and a completion action.

        """
        entity_id = None
        entity_type = None
        user_actions = []

        if event_type == 'problem_check':
            if event_source == 'server':
                entity_type = 'problem'
                if event_data.get('success', 'incorrect').lower() == 'correct':
                    user_actions.append('completed')

                user_actions.append('attempted')
                entity_id = event_data.get('problem_id')
        elif event_type == 'play_video':
            entity_type = 'video'
            user_actions.append('viewed')
            entity_id = event_data.get('id', '').strip()  # We have seen id values with leading newlines.
        elif event_type.startswith('edx.forum.'):
            entity_type = 'discussion'
            if event_type.endswith('.created'):
                user_actions.append('contributed')

            entity_id = event_data.get('commentable_id')

        return entity_id, entity_type, user_actions

    def get_raw_events_from_log_file(self, input_file):
        raw_events = []
        for line in input_file:
            event_row = self.mapper(line)
            if not event_row:
                continue
            raw_events.extend(event_row)
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
        columns = ['record_without_count', 'count']
        log.info('raw_events = {}'.format(raw_events))
        if len(raw_events) == 0:
            log.warn('raw_events is empty!')
            pass
        else:
            df = pd.DataFrame(data=raw_events, columns=columns)
            for (course_id, username, date, entity_type, entity_id, action), group in df.groupby(
                    ['record_without_count']):
                values = group.get_values()
                yield (course_id, username, date, entity_type, entity_id, action, len(values))

    def complete(self):
        return self.completed
        # if self.overwrite and not self.attempted_removal:
        #     return False
        # else:
        #     return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def init_local(self):
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def run(self):
        self.init_local()
        super(ModuleEngagementDataTask, self).run()
        if not self.completed:
            self.completed = True
        # self.remove_output_on_overwrite()
        # output_target = self.output()
        # if not self.complete() and output_target.exists():
        #     output_target.remove()
        # return super(ModuleEngagementDataTask, self).run()

    def requires(self):
        requires = super(ModuleEngagementDataTask, self).requires()
        if isinstance(requires, luigi.Task):
            yield requires

    def incr_counter(self, param, param1, param2):
        pass


class ModuleEngagementTableTask(ModuleEngagementDownstreamMixin, IncrementalMysqlTableInsertTask):
    """The hive table for this engagement data."""
    allow_empty_insert = False

    @property
    def table(self):
        return 'module_engagement'

    @property
    def columns(self):
        return ModuleEngagementRecord.get_sql_schema()

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    def requires_local(self):
        return ModuleEngagementDataTask(date=self.date)

    @property
    def record_filter(self):
        return "`date`='{date}'".format(date=self.date.isoformat())  # pylint: disable=no-member

    def requires(self):
        for req in super(ModuleEngagementTableTask, self).requires():
            yield req
        yield self.requires_local()


class ModuleEngagementIntervalTask(ModuleEngagementDownstreamMixin, WeekIntervalMixin, luigi.Task):
    """Compute engagement information over a range of dates and insert the results into Hive and MySQL"""

    overwrite_mysql = luigi.BooleanParameter(
        default=True,
        significant=False
    )

    completed = False

    def requires(self):
        log.info('self.interval = {}'.format(self.interval))
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            should_overwrite = date >= self.overwrite_from_date
            log.info('should_overwrite = {}, date = {}'.format(should_overwrite, date))
            yield ModuleEngagementTableTask(
                date=date,
                overwrite=should_overwrite,
            )

    def output(self):
        return [task.output() for task in self.requires()]

    def get_raw_data_tasks(self):
        """
        A generator that iterates through all tasks used to generate the data in each partition in the interval.

        This can be used by downstream map reduce jobs to read all of the raw data.
        """
        for task in self.requires():
            if isinstance(task, ModuleEngagementTableTask):
                data_task = task.requires_local()
                if isinstance(data_task, ModuleEngagementDataTask):
                    yield data_task

    def complete(self):
        return self.completed

    def run(self):
        log.info('ModuleEngagementIntervalTask running')
        if not self.completed:
            self.completed = True


#
#
# class ModuleEngagementSummaryDataTask(WeekIntervalMixin, ModuleEngagementDownstreamMixin, OverwriteOutputMixin,
#                                       luigi.Task):
#     completed = False
#
#     def complete(self):
#         return self.completed
#
#     def output(self):
#         require = self.requires_local()
#         if require:
#             for task in require.get_raw_data_tasks():
#                 for raw_events in task.output():
#                     columns = ['course_id', 'username', 'date', 'entity_type', 'entity_id', 'action', 'count']
#                     log.info('raw_events = {}'.format(raw_events))
#                     if len(raw_events) == 0:
#                         log.warn('raw_events is empty!')
#                         pass
#                     else:
#                         df = pd.DataFrame(data=raw_events, columns=columns)
#
#                         for (course_id, username), group in df.groupby(['course_id', 'username']):
#                             values = group[['date', 'entity_type', 'entity_id', 'action', 'count']].get_values()
#                             output_record_builder = ModuleEngagementSummaryRecordBuilder()
#                             for line in values:
#                                 record = ModuleEngagementRecord(course_id=course_id, username=username, date=line[0],
#                                                                 entity_type=line[1], entity_id=line[2],
#                                                                 event=line[3], count=line[4])
#                                 output_record_builder.add_record(record)
#
#                                 yield output_record_builder.get_summary_record(course_id, username,
#                                                                                self.interval).to_string_tuple()
#
#     def run(self):
#         log.info('ModuleEngagementSummaryDataTask running')
#         if not self.completed:
#             self.completed = True
#
#     def requires(self):
#         for req in super(ModuleEngagementSummaryDataTask, self).requires():
#             yield req
#         yield self.requires_local()
#
#     def requires_local(self):
#         return ModuleEngagementIntervalTask(
#             date=self.date,
#             overwrite_from_date=self.overwrite_from_date,
#         )
#

class ModuleEngagementSummaryTableTask(WeekIntervalMixin, ModuleEngagementDownstreamMixin, MysqlTableTask):
    """The hive table for this summary of engagement data."""

    @property
    def table(self):
        return 'module_engagement_summary'

    @property
    def columns(self):
        return ModuleEngagementSummaryRecord.get_sql_schema()

    def requires(self):
        for req in super(ModuleEngagementSummaryTableTask, self).requires():
            yield req
        yield self.requires_local()

    def requires_local(self):
        return ModuleEngagementIntervalTask(
            date=self.date,
            overwrite_from_date=self.overwrite_from_date,
        )

    @property
    def insert_query(self):
        query = """
            SELECT course_id,
                  username,
                  `date`,
                  entity_type,
                  entity_id,
                  `event`,
                  `count` 
            FROM module_engagement
            WHERE  `date` >= '{start_date}' AND `date` <= '{end_date}'
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
        )
        return query

    def rows(self):
        log.info('query_sql = [{}]'.format(self.insert_query))
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)

        columns = ['course_id', 'username', 'date', 'entity_type', 'entity_id', 'action', 'count']
        log.info('raw_events = {}'.format(query_result))
        df = pd.DataFrame(data=query_result, columns=columns)

        for (course_id, username), group in df.groupby(['course_id', 'username']):
            values = group[['date', 'entity_type', 'entity_id', 'action', 'count']].get_values()
            output_record_builder = ModuleEngagementSummaryRecordBuilder()
            for line in values:
                record = ModuleEngagementRecord(course_id=course_id, username=username, date=line[0],
                                                entity_type=line[1], entity_id=line[2],
                                                event=line[3], count=line[4])
                output_record_builder.add_record(record)

            yield output_record_builder.get_summary_record(course_id, username, self.interval).to_string_tuple()
        # for row in query_result:
        #     record = ModuleEngagementRecord(course_id=row[0], username=row[1], date=row[2],
        #                                     entity_type=row[3], entity_id=row[4],
        #                                     event=row[5], count=row[6])
        #     output_record_builder.add_record(record)
        #
        # yield output_record_builder.get_summary_record(row[0], row[1], self.interval).to_string_tuple()


class ModuleEngagementSummaryMetricRangesMysqlTask(ModuleEngagementDownstreamMixin, MysqlTableTask):
    """Result store storage for the metric ranges."""
    low_percentile = luigi.FloatParameter(default=15.0)
    high_percentile = luigi.FloatParameter(default=85.0)

    overwrite = luigi.BooleanParameter(
        default=True,
        description='Overwrite the table when writing to it by default. Allow users to override this behavior if they '
                    'want.',
        significant=False
    )
    allow_empty_insert = luigi.BooleanParameter(
        default=False,
        config_path={'section': 'module-engagement', 'name': 'allow_empty_insert'},
    )

    @property
    def table(self):
        return "module_engagement_metric_ranges"

    @property
    def columns(self):
        return ModuleEngagementSummaryMetricRangeRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id', 'metric'),
        ]

    @property
    def insert_query(self):
        query = """
            SELECT course_id,
                  username,
                  start_date,
                  end_date,
                  problem_attempts,
                  problems_attempted,
                  problems_completed,
                  problem_attempts_per_completed,
                  videos_viewed,
                  discussion_contributions,
                  days_active
            FROM module_engagement_summary
        """
        return query

    def rows(self):
        log.info('query_sql = [{}]'.format(self.insert_query))
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        columns = ['course_id', 'username', 'start_date', 'end_date', 'problem_attempts', 'problems_attempted',
                   'problems_completed', 'problem_attempts_per_completed', 'videos_viewed',
                   'discussion_contributions', ' days_active']

        log.info('raw_events = {}'.format(query_result))
        df = pd.DataFrame(data=query_result, columns=columns)

        for course_id, group in df.groupby(['course_id']):
            values = group[['username', 'start_date', 'end_date', 'problem_attempts', 'problems_attempted',
                            'problems_completed', 'problem_attempts_per_completed', 'videos_viewed',
                            'discussion_contributions', ' days_active']].get_values()

            metric_values = defaultdict(list)

            unprocessed_metrics = set()
            first_record = None
            for line in values:
                record = ModuleEngagementSummaryRecord(course_id=course_id, username=line[0], start_date=line[1],
                                                       end_date=line[2], problem_attempts=line[3],
                                                       problems_attempted=line[4], problems_completed=line[5],
                                                       problem_attempts_per_completed=line[6],
                                                       videos_viewed=line[7], discussion_contributions=line[8],
                                                       days_active=line[9])
                if first_record is None:
                    # There is some information we need to copy out of the summary records, so just grab one of them. There
                    # will be at least one, or else the reduce function would have never been called.
                    first_record = record

                # don't include inactive learners in metric range computations
                if record.days_active == 0:
                    continue

                for metric, value in record.get_metrics():
                    unprocessed_metrics.add(metric)
                    if metric == 'problem_attempts_per_completed' and record.problem_attempts == 0:
                        # The learner needs to have at least attempted one problem in order for their float('inf') to be
                        # included in the metric ranges. If the ratio is 0/0 we ignore the record.
                        continue
                    metric_values[metric].append(value)

            for metric in sorted(metric_values):
                unprocessed_metrics.remove(metric)
                values = metric_values[metric]
                normal_lower_bound, normal_upper_bound = numpy.percentile(  # pylint: disable=no-member
                    values, [self.low_percentile, self.high_percentile]
                )
                if numpy.isnan(normal_lower_bound):
                    normal_lower_bound = float('inf')
                if numpy.isnan(normal_upper_bound):
                    normal_upper_bound = float('inf')
                ranges = []
                if normal_lower_bound > 0:
                    ranges.append((METRIC_RANGE_LOW, 0, normal_lower_bound))

                if normal_lower_bound == normal_upper_bound:
                    ranges.append((METRIC_RANGE_NORMAL, normal_lower_bound, float('inf')))
                else:
                    ranges.append((METRIC_RANGE_NORMAL, normal_lower_bound, normal_upper_bound))
                    ranges.append((METRIC_RANGE_HIGH, normal_upper_bound, float('inf')))

                for range_type, low_value, high_value in ranges:
                    yield ModuleEngagementSummaryMetricRangeRecord(
                        course_id=course_id,
                        start_date=first_record.start_date,
                        end_date=first_record.end_date,
                        metric=metric,
                        range_type=range_type,
                        low_value=low_value,
                        high_value=high_value
                    ).to_string_tuple()

            for metric in unprocessed_metrics:
                yield ModuleEngagementSummaryMetricRangeRecord(
                    course_id=course_id,
                    start_date=first_record.start_date,
                    end_date=first_record.end_date,
                    metric=metric,
                    range_type='normal',
                    low_value=0,
                    high_value=float('inf')
                ).to_string_tuple()

    def requires(self):
        for req in super(ModuleEngagementSummaryMetricRangesMysqlTask, self).requires():
            yield req
        yield ModuleEngagementSummaryTableTask(
            date=self.date,
            overwrite_from_date=self.overwrite_from_date,
        )


class ModuleEngagementUserSegmentTableTask(ModuleEngagementDownstreamMixin, MysqlTableTask):
    """Hive table for user segment assignments."""

    @property
    def table(self):
        return 'module_engagement_user_segments'

    @property
    def columns(self):
        return ModuleEngagementUserSegmentRecord.get_sql_schema()

    @property
    def insert_query(self):
        query = """
            SELECT course_id,
                  username,
                  start_date,
                  end_date,
                  problem_attempts,
                  problems_attempted,
                  problems_completed,
                  problem_attempts_per_completed,
                  videos_viewed,
                  discussion_contributions,
                  days_active
            FROM module_engagement_summary
        """
        return query

    def rows(self):
        self.init_local()
        log.info('query_sql = [{}]'.format(self.insert_query))
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        columns = ['course_id', 'username', 'start_date', 'end_date', 'problem_attempts', 'problems_attempted',
                   'problems_completed', 'problem_attempts_per_completed', 'videos_viewed',
                   'discussion_contributions', ' days_active']

        log.info('raw_events = {}'.format(query_result))
        df = pd.DataFrame(data=query_result, columns=columns)

        for (course_id, username), group in df.groupby(['course_id', 'username']):
            values = group[['start_date', 'end_date', 'problem_attempts', 'problems_attempted',
                            'problems_completed', 'problem_attempts_per_completed', 'videos_viewed',
                            'discussion_contributions', ' days_active']].get_values()

            records = [ModuleEngagementSummaryRecord(course_id=course_id, username=username, start_date=line[0],
                                                     end_date=line[1], problem_attempts=line[2],
                                                     problems_attempted=line[6], problems_completed=line[4],
                                                     problem_attempts_per_completed=line[5] if line[5] else float(
                                                         'inf'),
                                                     videos_viewed=line[6], discussion_contributions=line[7],
                                                     days_active=line[8]) for line in values]

            if len(records) > 1:
                raise RuntimeError('There should be exactly one summary record per user per course.')

            summary = records[0]

            # Maps segment names to a set of "reasons" that tell why the user was placed in that segment
            segments = defaultdict(set)
            for metric, value in summary.get_metrics():
                high_metric_range = self.high_metric_ranges.get(course_id, {}).get(metric)
                if high_metric_range is None or metric == 'problem_attempts':
                    continue

                # Typically a left-closed interval, however, we consider infinite values to be included in the interval
                # if the upper bound is infinite.
                value_less_than_high = (
                        (value < high_metric_range.high_value) or
                        (value in (float('inf'), float('-inf')) and high_metric_range.high_value == value)
                )
                if (high_metric_range.low_value <= value) and value_less_than_high:
                    if metric == 'problem_attempts_per_completed':
                        # A high value for this metric actually indicates a struggling student
                        segments[SEGMENT_STRUGGLING].add(metric)
                    else:
                        segments[SEGMENT_HIGHLY_ENGAGED].add(metric)

            for segment in sorted(segments):
                yield ModuleEngagementUserSegmentRecord(
                    course_id=course_id,
                    username=username,
                    start_date=records[0].start_date,
                    end_date=records[0].end_date,
                    segment=segment,
                    reason=','.join(segments[segment])
                ).to_string_tuple()

    def requires(self):
        for req in super(ModuleEngagementUserSegmentTableTask, self).requires():
            yield req
        yield ModuleEngagementSummaryMetricRangesMysqlTask(
            date=self.date,
            overwrite_from_date=self.overwrite_from_date,
        )

    def init_local(self):
        self.high_metric_ranges = defaultdict(dict)
        query = """
        SELECT 
               course_id, 
               start_date, 
               end_date, 
               metric, 
               range_type, 
               low_value, 
               high_value
        FROM module_engagement_metric_ranges
        """
        log.info('query_sql = [{}]'.format(query))
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=query)
        for row in query_result:
            range_record = ModuleEngagementSummaryMetricRangeRecord(course_id=row[0],
                                                                    start_date=row[1],
                                                                    end_date=row[2],
                                                                    metric=row[3],
                                                                    range_type=row[4],
                                                                    low_value=row[5] if row[5] else float('inf'),
                                                                    high_value=row[6] if row[6] else float('inf'))
            if range_record.range_type == METRIC_RANGE_HIGH:
                self.high_metric_ranges[range_record.course_id][range_record.metric] = range_record


class CourseUserGroupSelectionTask(DatabaseImportMixin, luigi.Task):
    completed = False

    def __init__(self, *args, **kwargs):
        super(CourseUserGroupSelectionTask, self).__init__(*args, **kwargs)

    def complete(self):
        return self.completed
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        query = """
            SELECT
                    `id`,
                    `name`,
                    `course_id`,
                    `group_type`
            FROM course_groups_courseusergroup
            """
        return query

    def output(self):
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        for row in query_result:
            yield row

    def run(self):
        log.info('CourseUserGroupSelectionTask running')
        if not self.completed:
            self.completed = True

    def requires(self):
        yield ExternalURL(url=self.credentials)


class ImportCourseUserGroupTask(MysqlTableTask):
    """
    Imports user demographic information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """

    def __init__(self, *args, **kwargs):
        super(ImportCourseUserGroupTask, self).__init__(*args, **kwargs)

    @property
    def table(self):  # pragma: no cover
        return 'course_groups_courseusergroup'

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    @property
    def columns(self):
        return [
            ('courseusergroup_id', 'INT(11)'),
            ('name', 'VARCHAR(255)'),
            ('course_id', 'VARCHAR(255)'),
            ('group_type', 'VARCHAR(255)'),
        ]

    @property
    def indexes(self):
        return [
            ('courseusergroup_id',),
        ]

    def requires_local(self):
        return CourseUserGroupSelectionTask()

    def requires(self):
        for req in super(ImportCourseUserGroupTask, self).requires():
            yield req
        requires_local = self.requires_local()
        if isinstance(requires_local, luigi.Task):
            yield requires_local


class CourseUserGroupUsersSelectionTask(DatabaseImportMixin, luigi.Task):
    completed = False

    def __init__(self, *args, **kwargs):
        super(CourseUserGroupUsersSelectionTask, self).__init__(*args, **kwargs)

    def complete(self):
        return self.completed
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        query = """
            SELECT
                    courseusergroup_id,
                    user_id
            FROM course_groups_courseusergroup_users
            """
        return query

    def output(self):
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        for row in query_result:
            yield row

    def run(self):
        log.info('CourseUserGroupUsersSelectionTask running')
        if not self.completed:
            self.completed = True

    def requires(self):
        yield ExternalURL(url=self.credentials)


class ImportCourseUserGroupUsersTask(MysqlTableTask):
    """
    Imports user cohort information from an external LMS DB to both a
    destination directory and a HIVE metastore.

    """

    def __init__(self, *args, **kwargs):
        super(ImportCourseUserGroupUsersTask, self).__init__(*args, **kwargs)

    @property
    def table(self):  # pragma: no cover
        return 'course_groups_courseusergroup_users'

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    @property
    def columns(self):
        return [
            ('courseusergroup_id', 'INT(11)'),
            ('user_id', 'INT(11)'),
        ]

    def requires_local(self):
        return CourseUserGroupUsersSelectionTask()

    def requires(self):
        for req in super(ImportCourseUserGroupUsersTask, self).requires():
            yield req
        requires_local = self.requires_local()
        if isinstance(requires_local, luigi.Task):
            yield requires_local


class ModuleEngagementRosterPartitionTask(WeekIntervalMixin, ModuleEngagementDownstreamMixin, MysqlTableTask):
    """
    A Hive partition that represents the roster as of a particular day.

    Note that data from the prior 2 weeks is used to generate the summary for a particular date.
    """

    date = luigi.DateParameter()
    max_field_length = luigi.IntParameter(
        description='If set, truncate any long strings from the auth_userprofile table to this maximum length. '
                    ' This is required for ElasticSearch, which throws a MaxBytesLengthExceededException for any term '
                    ' that is longer than its configured max length.',
        default=20000,
    )

    interval = None
    partition_value = None

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementRosterPartitionTask, self).__init__(*args, **kwargs)

    @property
    def insert_query(self):
        # The end of the interval is not closed, so use the prior day's enrollment data.
        last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member

        def strip_and_truncate(field):
            """
            Identify delimiters in the data and strip them out to prevent parsing errors.

            Also, if self.max_field_length is set, then truncate the field to self.max_field_length.
            """
            stripped = "regexp_replace(regexp_replace({}, '\\\\t|\\\\n|\\\\r', ' '), '\\\\\\\\', '')".format(field)

            if self.max_field_length is not None:
                stripped = "substring({}, 1, {})".format(stripped, self.max_field_length)
            return stripped

        query = """
        SELECT
            ce.course_id,
            au.username,
            '{start}',
            '{end}',
            au.email,
            {aup_name},
            ce.mode,
            lce.first_enrollment_date,
            cohort.name,
            COALESCE(eng.problem_attempts, 0),
            COALESCE(eng.problems_attempted, 0),
            COALESCE(eng.problems_completed, 0),
            eng.problem_attempts_per_completed,
            COALESCE(eng.videos_viewed, 0),
            COALESCE(eng.discussion_contributions, 0),
            CONCAT_WS(
                ",",
                IF(ce.at_end = 0, "unenrolled", NULL),
                -- The learner has had no activity in the past 2 weeks.
                IF(COALESCE(old_eng.days_active, 0) = 0 AND COALESCE(eng.days_active, 0) = 0, "inactive", NULL),
                -- Two weeks ago the learner was active, however, they have had no activity in the most recent week.
                IF(COALESCE(old_eng.days_active, 0) > 0 AND COALESCE(eng.days_active, 0) = 0, "disengaging", NULL),
                seg.segments
            ),
            -- attempt_ratio_order
            -- This field is a secondary sort key for the records in this table. Combined with a primary sort on
            -- problem_attempts_per_completed it can be used to identify top performers or struggling learners. It
            -- provides a magnitude for the degree to which a user is performing well or poorly. If, for example, they
            -- have made 10 attempts and gotten 10 problems correct, we want to sort that learner as higher
            -- performing than a user who has only made 1 attempt on one problem and was correct. Similarly, if a user
            -- has made 10 attempts without getting any problems correct, we want to sort that learner as lower
            -- performing than a user who has only made one attempt without getting the problem correct.

            -- To see high performing learners sort by (problem_attempts_per_completed ASC, attempt_ratio_order DESC)
            -- To see struggling learners sort by (problem_attempts_per_completed DESC, attempt_ratio_order ASC)
            IF(
                eng.problem_attempts_per_completed IS NULL,
                -COALESCE(eng.problem_attempts, 0),
                COALESCE(eng.problem_attempts, 0)
            ),
            aup.user_id,
            {aup_language},
            {aup_location},
            aup.year_of_birth,
            {aup_level_of_education},
            {aup_gender},
            {aup_mailing_address},
            {aup_city},
            {aup_country},
            {aup_goals}
        FROM course_enrollment ce
        INNER JOIN auth_user au
            ON (ce.user_id = au.id)
        INNER JOIN auth_userprofile aup
            ON (au.id = aup.user_id)
        LEFT OUTER JOIN (
            SELECT
                cugu.user_id,
                cug.course_id,
                cug.name
            FROM course_groups_courseusergroup_users cugu
            INNER JOIN course_groups_courseusergroup cug
                ON (cugu.courseusergroup_id = cug.id)
        ) cohort
            ON (au.id = cohort.user_id AND ce.course_id = cohort.course_id)
        LEFT OUTER JOIN module_engagement_summary eng
            ON (ce.course_id = eng.course_id AND au.username = eng.username AND eng.end_date = '{end}')
        LEFT OUTER JOIN module_engagement_summary old_eng
            ON (ce.course_id = old_eng.course_id AND au.username = old_eng.username AND old_eng.end_date = DATE_SUB('{end}', 7))
        LEFT OUTER JOIN (
            SELECT
                course_id,
                user_id,
                MIN(`date`) AS first_enrollment_date
            FROM course_enrollment
            WHERE
                at_end = 1 AND `date` < '{end}'
            GROUP BY course_id, user_id
        ) lce
            ON (ce.course_id = lce.course_id AND ce.user_id = lce.user_id)
        LEFT OUTER JOIN (
            SELECT
                course_id,
                username,
                CONCAT_WS(",", COLLECT_SET(segment)) AS segments
            FROM module_engagement_user_segments
            WHERE end_date = '{end}'
            GROUP BY course_id, username
        ) seg
            ON (ce.course_id = seg.course_id AND au.username = seg.username)
        WHERE
            ce.`date` = '{last_complete_date}'
        """.format(
            start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
            end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
            last_complete_date=last_complete_date.isoformat(),
            aup_name=strip_and_truncate('aup.name'),
            aup_language=strip_and_truncate('aup.language'),
            aup_location=strip_and_truncate('aup.location'),
            aup_level_of_education=strip_and_truncate('aup.level_of_education'),
            aup_gender=strip_and_truncate('aup.gender'),
            aup_mailing_address=strip_and_truncate('aup.mailing_address'),
            aup_city=strip_and_truncate('aup.city'),
            aup_country=strip_and_truncate('aup.country'),
            aup_goals=strip_and_truncate('aup.goals'),
        )
        return query

    @property
    def table(self):
        return 'module_engagement_roster'

    @property
    def columns(self):
        return ModuleEngagementRosterRecord.get_sql_schema()

    def requires(self):
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
            'import_date': self.date
        }
        yield (
            ModuleEngagementSummaryMetricRangesMysqlTask(
                date=self.date,
                overwrite_from_date=self.overwrite_from_date,
            ),
            ModuleEngagementUserSegmentTableTask(
                date=self.date,
                overwrite_from_date=self.overwrite_from_date,
            ),
            # ExternalCourseEnrollmentPartitionTask(
            #     interval_end=self.date
            # ),
            # CourseEnrollmentTask(
            #     overwrite_mysql=self.overwrite_mysql,
            #     source=self.source,
            #     interval=self.interval,
            #     pattern=self.pattern,
            #     overwrite_n_days=self.overwrite_n_days
            # )
            ImportAuthUserTask(),
            ImportCourseUserGroupTask(),
            ImportCourseUserGroupUsersTask(),
            ImportAuthUserProfileTask(),
        )


NAMES = ['james', 'john', 'robert', 'william', 'michael', 'david', 'richard', 'charles', 'joseph', 'thomas',
         'mary', 'patricia', 'linda', 'barbara', 'elizabeth', 'jennifer', 'maria', 'susan', 'margaret', 'dorothy']
SURNAMES = ['smith', 'johnson', 'williams', 'jones', 'brown', 'davis', 'miller', 'wilson', 'moore', 'taylor']


@workflow_entry_point  # pylint: disable=missing-docstring
class HylModuleEngagementWorkflowTask(ModuleEngagementDownstreamMixin, ModuleEngagementRosterIndexDownstreamMixin,
                                      luigi.WrapperTask):
    __doc__ = """
    A rapidly searchable learner roster for each course with aggregate statistics about that learner's performance.

    Each record written to the elasticsearch index represents a single learner's performance in the course in the last
    week. Note that the index will contain records for users that have since unenrolled in the course, so there will
    be one record for every user who has ever been enrolled in that course.

    Given that each week the learner's statistics may have changed, the entire index is re-written every time this task
    is run. Elasticsearch does not support transactional updates. For this reason, we write to a separate index
    on each update, using an alias to point to the "live" index while we build the next version. Once the next version
    is complete, we switch over the alias to point to the newly built index and drop the old one. This allows us to
    continue to expose a consistent view of the roster while we are writing out a new version. Without an atomic toggle
    like this, it is possible an instructor could run a query and see a mix of data from the old and new versions of
    the index.

    For more information about this strategy see `Index Aliases and Zero Downtime`_.

    We organize the data in a single index that contains the data for all courses. We rely on the default
    elasticsearch sharding strategy instead of manually attempting to shard the data by course (or some other
    dimension). This choice was made largely because it is simpler to implement and manage.

    The index is optimized for the following access patterns within a single course:

    - Find learners by some part of their name. For example: "John" or "John Doe".
    - Find learners by their exact username. For example: "johndoe". Note that partial matches are not supported when
      searching by username. A query of "john" will not match the username "johndoe".
    - Find learners by their exact email address. For example: "johndoe@gmail.com".
    - Find learners by the segments they belong to. For example: "disengaging AND struggling". Note that these segments
      can be combined using arbitrary boolean logic "(disengaging AND struggling) OR highly_engaged".

    Each record contains the following fields: {record_doc}

    This workflow also generates two relational database tables in the result store:

    1. The `module_engagement` table which has one record per type of learner interaction with each module in the course
       in a given day. For example, if a learner clicked play on a video 4 times in a single day, there would be one
       record in this table that represented that interaction. It would contain the course, the date, the ID of the
       video, the learner's username, and the fact that they pressed play 4 times. Each record contains the following
       fields: {engagement_record_doc}

    2. The `module_engagement_metric_ranges` table which has one record per course metric range. These ranges specify
       various thresholds for bucketing user activity. Typically "low", "normal" or "high". Each record contains the
       following fields: {ranges_record_doc}

    .. _Index Aliases and Zero Downtime: https://www.elastic.co/guide/en/elasticsearch/guide/1.x/index-aliases.html
    """.format(
        record_doc=ModuleEngagementRosterRecord.get_restructured_text(),
        engagement_record_doc=ModuleEngagementRecord.get_restructured_text(),
        ranges_record_doc=ModuleEngagementSummaryMetricRangeRecord.get_restructured_text(),
    )

    overwrite_from_date = None
    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'module_engagement', 'name': 'overwrite_n_days'},
        significant=False,
        default=3,
        description='This parameter is passed down to the module engagement model which will overwrite data from a date'
                    ' in the past up to the end of the interval. Events are not always collected at the time that they'
                    ' are emitted, sometimes much later. By re-processing past days we can gather late events and'
                    ' include them in the computations. This parameter specifies the number of days to overwrite'
                    ' starting with the most recent date. A value of 0 indicates no days should be overwritten.'
    )

    # Don't use the OverwriteOutputMixin since it changes the behavior of complete() (which we don't want).
    overwrite = luigi.BooleanParameter(default=False, significant=False)
    throttle = luigi.FloatParameter(
        config_path={'section': 'module-engagement', 'name': 'throttle'},
        description=ElasticsearchIndexTask.throttle.description,
        default=0.75,
        significant=False
    )

    def requires(self):
        overwrite_from_date = self.date - datetime.timedelta(days=self.overwrite_n_days)
        # return ModuleEngagementSummaryTableTask(
        #     date=self.date,
        #     overwrite_from_date=overwrite_from_date,
        # )
        # yield ModuleEngagementRosterIndexTask(
        #     date=self.date,
        #     indexing_tasks=self.indexing_tasks,
        #     scale_factor=self.scale_factor,
        #     obfuscate=self.obfuscate,
        #     overwrite_from_date=overwrite_from_date,
        #     overwrite=self.overwrite,
        #     throttle=self.throttle
        # )
        #
        # yield ImportCourseUserGroupTask()
        # yield ImportCourseUserGroupUsersTask()
        yield ModuleEngagementRosterPartitionTask(
            date=self.date,
            overwrite_from_date=overwrite_from_date,
        )

    def output(self):
        return [t.output() for t in self.requires()]
