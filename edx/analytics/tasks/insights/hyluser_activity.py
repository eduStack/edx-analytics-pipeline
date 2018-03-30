"""Categorize activity of users."""

import datetime
import gzip
import logging
from collections import Counter

import luigi
import luigi.date_interval

import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.util.data import LoadEventFromMongoTask
from edx.analytics.tasks.common.mysql_load import get_mysql_query_results, IncrementalMysqlTableInsertTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.insights.calendar_task import MysqlCalendarTableTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class UserActivityTask(OverwriteOutputMixin, LoadEventFromMongoTask):
    """
      Categorize activity of users.

      Analyze the history of user actions and categorize their activity. Note that categories are not mutually exclusive.
      A single event may belong to multiple categories. For example, we define a generic "ACTIVE" category that refers
      to any event that has a course_id associated with it, but is not an enrollment event. Other events, such as a
      video play event, will also belong to other categories.

      The output from this job is a table that represents the number of events seen for each user in each course in each
      category on each day.

      """

    def get_predicate_labels(self, event):
        """Creates labels by applying hardcoded predicates to a single event."""
        # We only want the explicit event, not the implicit form.
        event_type = event.get('event_type')
        event_source = event.get('event_source')

        # Ignore all background task events, since they don't count as a form of activity.
        if event_source == 'task':
            return []

        # Ignore all enrollment events, since they don't count as a form of activity.
        if event_type.startswith('edx.course.enrollment.'):
            return []

        labels = [ACTIVE_LABEL]

        if event_source == 'server':
            if event_type == 'problem_check':
                labels.append(PROBLEM_LABEL)

            if event_type.startswith('edx.forum.') and event_type.endswith('.created'):
                labels.append(POST_FORUM_LABEL)

        if event_source in ('browser', 'mobile'):
            if event_type == 'play_video':
                labels.append(PLAY_VIDEO_LABEL)

        return labels

    def _encode_tuple(self, values):
        """
        Convert values into a tuple containing encoded strings.

        Parameters:
            Values is a list or tuple.

        This enforces a standard encoding for the parts of the
        key. Without this a part of the key might appear differently
        in the key string when it is coerced to a string by luigi. For
        example, if the same key value appears in two different
        records, one as a str() type and the other a unicode() then
        without this change they would appear as u'Foo' and 'Foo' in
        the final key string. Although python doesn't care about this
        difference, hadoop does, and will bucket the values
        separately. Which is not what we want.
        """
        # TODO: refactor this into a utility function and update jobs
        # to always UTF8 encode mapper keys.
        if len(values) > 1:
            return tuple([value.encode('utf8') for value in values])
        else:
            return values[0].encode('utf8')

    def event_filter(self):
        return {
            '$and': [
                {'timestamp': {'$lte': self.upper_bound_date_timestamp}},
                {'timestamp': {'$gte': self.lower_bound_date_timestamp}},
            ]}
        # {'event_type': {'$in': ['problem_check', 'play_video']}}

    def get_event_row_from_document(self, document):
        event_and_date_string = self.get_event_and_date_string(document)
        if not event_and_date_string:
            return
        event, date_string = event_and_date_string

        username = event.get('username', '').strip()
        if not username:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        events = []
        for label in self.get_predicate_labels(event):
            event_row = self._encode_tuple((course_id, username, date_string, label))
            events.append(event_row)
        return events

    def processing(self, raw_events):
        counter = Counter(raw_events)
        for key, num_events in counter.iteritems():
            course_id, username, date_string, label = key
            yield (course_id, username, date_string, label, num_events)


# class UserActivityTask(OverwriteOutputMixin, EventLogSelectionMixin, luigi.Task):
#     """
#     Categorize activity of users.
#
#     Analyze the history of user actions and categorize their activity. Note that categories are not mutually exclusive.
#     A single event may belong to multiple categories. For example, we define a generic "ACTIVE" category that refers
#     to any event that has a course_id associated with it, but is not an enrollment event. Other events, such as a
#     video play event, will also belong to other categories.
#
#     The output from this job is a table that represents the number of events seen for each user in each course in each
#     category on each day.
#
#     """
#     completed = False
#
#     def get_predicate_labels(self, event):
#         """Creates labels by applying hardcoded predicates to a single event."""
#         # We only want the explicit event, not the implicit form.
#         event_type = event.get('event_type')
#         event_source = event.get('event_source')
#
#         # Ignore all background task events, since they don't count as a form of activity.
#         if event_source == 'task':
#             return []
#
#         # Ignore all enrollment events, since they don't count as a form of activity.
#         if event_type.startswith('edx.course.enrollment.'):
#             return []
#
#         labels = [ACTIVE_LABEL]
#
#         if event_source == 'server':
#             if event_type == 'problem_check':
#                 labels.append(PROBLEM_LABEL)
#
#             if event_type.startswith('edx.forum.') and event_type.endswith('.created'):
#                 labels.append(POST_FORUM_LABEL)
#
#         if event_source in ('browser', 'mobile'):
#             if event_type == 'play_video':
#                 labels.append(PLAY_VIDEO_LABEL)
#
#         return labels
#
#     def _encode_tuple(self, values):
#         """
#         Convert values into a tuple containing encoded strings.
#
#         Parameters:
#             Values is a list or tuple.
#
#         This enforces a standard encoding for the parts of the
#         key. Without this a part of the key might appear differently
#         in the key string when it is coerced to a string by luigi. For
#         example, if the same key value appears in two different
#         records, one as a str() type and the other a unicode() then
#         without this change they would appear as u'Foo' and 'Foo' in
#         the final key string. Although python doesn't care about this
#         difference, hadoop does, and will bucket the values
#         separately. Which is not what we want.
#         """
#         # TODO: refactor this into a utility function and update jobs
#         # to always UTF8 encode mapper keys.
#         if len(values) > 1:
#             return tuple([value.encode('utf8') for value in values])
#         else:
#             return values[0].encode('utf8')
#
#     def get_raw_events_from_log_file(self, input_file):
#         raw_events = []
#         for line in input_file:
#             event_row = self.get_event_and_date_string(line)
#             if not event_row:
#                 continue
#             event, date_string = event_row
#
#             username = event.get('username', '').strip()
#             if not username:
#                 continue
#
#             course_id = eventlog.get_course_id(event)
#             if not course_id:
#                 continue
#
#             for label in self.get_predicate_labels(event):
#                 event_row = self._encode_tuple((course_id, username, date_string, label))
#                 raw_events.append(event_row)
#         return raw_events
#
#     def output(self):
#         raw_events = []
#         for log_file in luigi.task.flatten(self.input()):
#             with log_file.open('r') as temp_file:
#                 with gzip.GzipFile(fileobj=temp_file) as input_file:
#                     log.info('reading log file={}'.format(input_file))
#                     events = self.get_raw_events_from_log_file(input_file)
#                     if not events:
#                         continue
#                     raw_events.extend(events)
#         # (date_string, self._encode_tuple((course_id, username, date_string, label)))
#         # return [
#         #     ('course_id', 'VARCHAR(255) NOT NULL'),
#         #     ('username', 'VARCHAR(255) NOT NULL'),
#         #     ('date', 'DATE NOT NULL'),
#         #     ('category', 'VARCHAR(255) NOT NULL'),
#         #     ('count', 'INT(11) NOT NULL'),
#         # ]
#         counter = Counter(raw_events)
#         for key, num_events in counter.iteritems():
#             course_id, username, date_string, label = key
#             yield (course_id, username, date_string, label, num_events)
#
#     def init_local(self):
#         self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
#         self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member
#
#     def run(self):
#         self.init_local()
#         super(UserActivityTask, self).run()
#         if not self.completed:
#             self.completed = True
#
#     def complete(self):
#         return self.completed
#
#     def requires(self):
#         requires = super(UserActivityTask, self).requires()
#         if isinstance(requires, luigi.Task):
#             yield requires


class UserActivityDownstreamMixin(EventLogSelectionDownstreamMixin):
    """All parameters needed to run the UserActivityTableTask task."""

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        description='This parameter is used by UserActivityTask which will overwrite user-activity counts '
                    'for the most recent n days. Default is pulled from user-activity.overwrite_n_days.',
        significant=False,
    )


class UserActivityTableTask(UserActivityDownstreamMixin, IncrementalMysqlTableInsertTask):
    """
    The hive table for storing user activity data. This task also adds partition metadata info to the Hive metastore.
    """

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

    @property
    def record_filter(self):
        if self.overwrite:
            return """`date` >= '{}' AND `date` <= '{}'""".format(self.interval.date_a.isoformat(),
                                                                  self.interval.date_b.isoformat())
        else:
            return None

    @property
    def table(self):
        return 'user_activity'

    @property
    def indexes(self):
        return [
            ('course_id', 'date'),
        ]

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('username', 'VARCHAR(255) NOT NULL'),
            ('date', 'DATE NOT NULL'),
            ('category', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    def requires_local(self):
        return UserActivityTask(
            interval=self.interval,
            overwrite=True
        )

    def requires(self):
        # Overwrite n days of user activity data before recovering partitions.
        yield self.requires_local()


@workflow_entry_point
class HylInsertToMysqlCourseActivityTask(WeeklyIntervalMixin, UserActivityDownstreamMixin,
                                         IncrementalMysqlTableInsertTask):
    """
    Creates/populates the `course_activity` Result store table.
    """
    overwrite_mysql = luigi.BooleanParameter(
        default=True,
        description='Whether or not to overwrite the MySQL output objects; set to True by default.',
        significant=True
    )
    allow_empty_insert = luigi.BooleanParameter(
        default=True,
        config_path={'section': 'user-activity', 'name': 'allow_empty_insert'},
    )
    overwrite = None

    def __init__(self, *args, **kwargs):
        super(HylInsertToMysqlCourseActivityTask, self).__init__(*args, **kwargs)
        self.overwrite = self.overwrite_mysql

    @property
    def table(self):
        return "course_activity"

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        query = """
        SELECT
            act.course_id as course_id,
            CONCAT(cal.iso_week_start, ' 00:00:00') as interval_start,
            CONCAT(cal.iso_week_end, ' 00:00:00') as interval_end,
            act.category as label,
            COUNT(DISTINCT username) as `count`
        FROM user_activity act
        JOIN calendar cal
            ON act.`date` = cal.`date` AND act.`date` >= "{interval_start}" AND act.`date` < "{interval_end}"
        WHERE
            "{interval_start}" <= cal.`date` AND cal.`date` < "{interval_end}"
        GROUP BY
            act.course_id,
            cal.iso_week_start,
            cal.iso_week_end,
            act.category;
        """.format(
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat(),
        )

        return query

    @property
    def record_filter(self):
        if self.overwrite:
            return """`interval_end` >= '{}' AND `interval_end` <= '{}'""".format(self.interval.date_a.isoformat(),
                                                                                  self.interval.date_b.isoformat())
        else:
            return None

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('interval_start', 'DATETIME NOT NULL'),
            ('interval_end', 'DATETIME NOT NULL'),
            ('label', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'label'),
            ('interval_end',)
        ]

    def requires(self):
        overwrite_interval = None
        if self.overwrite_n_days > 0:
            overwrite_from_date = self.end_date - datetime.timedelta(days=self.overwrite_n_days)
            overwrite_interval = luigi.date_interval.Custom(overwrite_from_date, self.end_date)
        yield (
            UserActivityTableTask(
                interval=self.interval if overwrite_interval is None else overwrite_interval,
                overwrite=True,
            ),
            MysqlCalendarTableTask()
        )
