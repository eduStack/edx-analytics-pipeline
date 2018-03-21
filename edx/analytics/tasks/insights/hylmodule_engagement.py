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
from edx.analytics.tasks.common.mysql_load import IncrementalMysqlTableInsertTask, MysqlTableTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserProfileTask, ImportAuthUserTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask
)
from edx.analytics.tasks.insights.enrollments import ExternalCourseEnrollmentPartitionTask
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin, hive_database_name
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, FloatField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

try:
    import numpy
except ImportError:
    numpy = None  # pylint: disable=invalid-name

log = logging.getLogger(__name__)

METRIC_RANGE_HIGH = 'high'
METRIC_RANGE_NORMAL = 'normal'
METRIC_RANGE_LOW = 'low'


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


class ModuleEngagementDownstreamMixin(WarehouseMixin, MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin,
                                      OverwriteFromDateMixin):
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
    course_id = StringField(description='Course the learner is enrolled in.')
    username = StringField(description='Learner\'s username.')
    start_date = DateField(description='Analysis includes all data from 00:00 on this day up to the end date.')
    end_date = DateField(description='Analysis includes all data up to but not including this date.')
    email = StringField(description='Learner\'s email address.')
    name = StringField(analyzed=True, description='Learner\'s full name including first, middle and last names. '
                                                  'This field can be searched by instructors.')
    enrollment_mode = StringField(description='Learner is enrolled in the course with this mode. Example: verified.')
    enrollment_date = DateField(description='First date the learner enrolled in the course.')
    cohort = StringField(description='Cohort the learner belongs to, can be null.')
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
    language = StringField(description='Learner\'s preferred language.')
    location = StringField(description='Learner\'s reported location.')
    year_of_birth = IntegerField(description='Learner\'s reported year of birth.')
    level_of_education = StringField(description='Learner\'s reported level of education.')
    gender = StringField(description='Learner\'s reported gender.')
    mailing_address = StringField(description='Learner\'s reported mailing address.')
    city = StringField(description='Learner\'s reported city.')
    country = StringField(description='Learner\'s reported country.')
    goals = StringField(description='Learner\'s reported goals.')


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
        columns = ['record_without_count', 'count']
        log.info('raw_events = {}'.format(raw_events))

        df = pd.DataFrame(data=raw_events, columns=columns)
        for ((course_id, username, date, entity_type, entity_id, action), count), group in df.groupby(
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


class ModuleEngagementIntervalTask(EventLogSelectionDownstreamMixin, OverwriteOutputMixin, OverwriteFromDateMixin,
                                   luigi.WrapperTask):
    """Compute engagement information over a range of dates and insert the results into Hive and MySQL"""

    overwrite_mysql = luigi.BooleanParameter(
        default=True,
        significant=False
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            should_overwrite = date >= self.overwrite_from_date
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
                yield task


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
        return ModuleEngagementIntervalTask(
            interval=self.interval,
            overwrite_from_date=overwrite_from_date,
        )
        # yield ModuleEngagementRosterIndexTask(
        #     date=self.date,
        #     indexing_tasks=self.indexing_tasks,
        #     scale_factor=self.scale_factor,
        #     obfuscate=self.obfuscate,
        #     overwrite_from_date=overwrite_from_date,
        #     overwrite=self.overwrite,
        #     throttle=self.throttle
        # )
        # yield ModuleEngagementSummaryMetricRangesMysqlTask(
        #     date=self.date,
        #     overwrite_from_date=overwrite_from_date,
        #     n_reduce_tasks=self.n_reduce_tasks,
        # )

    def output(self):
        return [t.output() for t in self.requires()]
