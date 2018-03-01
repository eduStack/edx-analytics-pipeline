"""Compute metrics related to user enrollments in courses"""

import datetime
import logging

import luigi.task
from luigi import Task
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask, IncrementalMysqlInsertTask, get_mysql_query_results

from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.record import BooleanField, DateField, DateTimeField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import ExternalURL, UncheckedExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.common.pathutil import (
    EventLogSelectionDownstreamMixin, EventLogSelectionMixin, PathSelectionByDateIntervalTask
)

log = logging.getLogger(__name__)
DEACTIVATED = 'edx.course.enrollment.deactivated'
ACTIVATED = 'edx.course.enrollment.activated'
MODE_CHANGED = 'edx.course.enrollment.mode_changed'
ENROLLED = 1
UNENROLLED = 0


class OverwriteMysqlDownstreamMixin(object):
    """This mixin covers controls when we have both mysql objects eligible for overwriting."""

    overwrite_mysql = luigi.BooleanParameter(
        default=True,
        description='Whether or not to overwrite the MySQL output objects; set to True by default.',
        significant=True
    )


class CourseEnrollmentDownstreamMixin(EventLogSelectionDownstreamMixin):
    """All parameters needed to run the CourseEnrollmentTask task."""

    # Make the interval be optional:
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to extract enrollments events for. '
                    'If not specified, `interval_start` and `interval_end` are used to construct the `interval`.',
    )

    # Define optional parameters, to be used if 'interval' is not defined.
    interval_start = luigi.DateParameter(
        config_path={'section': 'enrollments', 'name': 'interval_start'},
        significant=False,
        description='The start date to extract enrollments events for.  Ignored if `interval` is provided.',
    )
    interval_end = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        significant=False,
        description='The end date to extract enrollments events for.  Ignored if `interval` is provided. '
                    'Default is today, UTC.',
    )

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'enrollments', 'name': 'overwrite_n_days'},
        significant=False,
        description='This parameter is used by CourseEnrollmentTask which will overwrite course enrollment '
                    ' events for the most recent n days.'
    )

    @property
    def query_date(self):
        """We want to store demographics breakdown from the enrollment numbers of most recent day only."""
        query_date = self.interval.date_b - datetime.timedelta(days=1)
        return query_date.isoformat()

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentDownstreamMixin, self).__init__(*args, **kwargs)

        if not self.interval:
            self.interval = luigi.date_interval.Custom(self.interval_start, self.interval_end)


class CourseEnrollmentRecord(Record):
    """A user's enrollment history."""
    date = DateField(nullable=False, description='Enrollment date.')
    course_id = StringField(length=255, nullable=False, description='The course the learner is enrolled in.')
    user_id = IntegerField(description='The user_id of the learner.')
    at_end = BooleanField(description='An indicator if the learner is still enrolled in the course at then end of this '
                                      'date.')
    change = BooleanField(description='')
    mode = StringField(length=255, description='')


class EnrollmentDailyRecord(Record):
    """Summarizes a course's enrollment by date."""
    course_id = StringField(length=255, nullable=False, description='The course the learners are enrolled in.')
    date = DateField(nullable=False, description='Enrollment date.')
    count = IntegerField(description='The number of learners in the course on this date.')
    cumulative_count = IntegerField(description='The count of learners that ever enrolled in this course on or before '
                                                'this date.')


class CourseEnrollmentEventsTask(EventLogSelectionMixin, luigi.Task):
    """
    Task to extract enrollment events from eventlogs over a given interval.
    This would produce a different output file for each day within the interval
    containing that day's enrollment events only.
    """
    complete = False
    # FILEPATH_PATTERN should match the output files defined by output_path_for_key().
    FILEPATH_PATTERN = '.*?course_enrollment_events_(?P<date>\\d{4}-\\d{2}-\\d{2})'

    # We use warehouse_path to generate the output path, so we make this a non-param.
    output_root = None

    counter_category_name = 'Enrollment Events'

    def output(self):
        rows = [
            ('2018-02-02', 'courseid1', '123123', True, True, '1'),
            ('2018-02-02', 'courseid1', '123123', True, True, '1'),
            ('2018-02-03', 'courseid1', '567567', True, True, '1'),
            ('2018-02-03', 'courseid1', '567567', True, True, '1'),
            ('2018-02-03', 'courseid1', '567567', True, True, '1'),
            ('2018-02-03', 'courseid1', '567567', True, True, '1'),
            ('2018-02-04', 'courseid1', '789789', True, True, '1'),
            ('2018-02-04', 'courseid1', '789789', True, True, '1'),
            ('2018-02-04', 'courseid1', '789789', True, True, '1'),
            ('2018-02-04', 'courseid1', '789789', True, True, '1'),
            ('2018-02-04', 'courseid1', '789789', True, True, '1')
        ]
        for row in rows:
            yield row

    def complete(self):
        return self.complete
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        log.info('test-run')
        if not self.complete:
            self.complete = True


class CourseEnrollmentTask(OverwriteMysqlDownstreamMixin, CourseEnrollmentDownstreamMixin, IncrementalMysqlInsertTask):
    """Produce a data set that shows which days each user was enrolled in each course."""

    overwrite = None

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentTask, self).__init__(*args, **kwargs)
        self.overwrite = self.overwrite_mysql

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def table(self):  # pragma: no cover
        return 'course_enrollment'

    def rows(self):
        for row in self.requires_local().output():
            yield row

    @property
    def columns(self):
        return CourseEnrollmentRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
            # Note that the order here is extremely important. The API query pattern needs to filter first by course and
            # then by date.
            ('course_id', 'date'),
        ]

    @property
    def record_filter(self):
        if self.overwrite:
            return """`date` >= '{}' AND `date` <= '{}'""".format(self.interval.date_a.isoformat(),
                                                                  self.interval.date_b.isoformat())
        else:
            return None
        # return """`date=`=`query_date`""".format(query_date=self.query_date)

    def requires_local(self):
        return CourseEnrollmentEventsTask(
            interval=self.overwrite_mysql,
            source=self.source,
            pattern=self.pattern
        )

    def requires(self):
        for requirement in super(CourseEnrollmentTask, self).requires().itervalues():
            yield requirement

        yield self.requires_local()


class EnrollmentDailyMysqlTask(OverwriteMysqlDownstreamMixin, CourseEnrollmentDownstreamMixin,
                               IncrementalMysqlInsertTask):
    """
    A history of the number of students enrolled in each course at the end of each day.

    During operations: The object at insert_source_task is opened and each row is treated as a row to be inserted.
    At the end of this task data has been written to MySQL.  Overwrite functionality is complex and configured through
    the OverwriteHiveAndMysqlDownstreamMixin, so we default the standard overwrite parameter to None.
    """
    overwrite = None

    def __init__(self, *args, **kwargs):
        super(EnrollmentDailyMysqlTask, self).__init__(*args, **kwargs)
        self.overwrite = self.overwrite_mysql

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def table(self):  # pragma: no cover
        return 'course_enrollment_daily'

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        query = """
            SELECT
                ce.course_id,
                ce.`date`,
                SUM(ce.at_end),
                COUNT(ce.user_id)
            FROM course_enrollment ce
            WHERE `date` >= '{}' AND `date` <= '{}'
            GROUP BY
                ce.course_id,
                ce.`date`
        """.format(self.interval.date_a.isoformat(), self.interval.date_b.isoformat())
        return query

    def rows(self):
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        log.info('query_sql = [{}]'.format(self.insert_query))
        for row in query_result:
            yield row

    @property
    def columns(self):
        return EnrollmentDailyRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id',),
            # Note that the order here is extremely important. The API query pattern needs to filter first by course and
            # then by date.
            ('course_id', 'date'),
        ]

    @property
    def record_filter(self):
        if self.overwrite:
            return """`date` >= '{}' AND `date` <= '{}'""".format(self.interval.date_a.isoformat(),
                                                                  self.interval.date_b.isoformat())
        else:
            return None

    def requires(self):
        for requirement in super(EnrollmentDailyMysqlTask, self).requires().itervalues():
            yield requirement

        # the process that generates the source table used by this query
        yield (
            CourseEnrollmentTask(
                overwrite_mysql=self.overwrite_mysql,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                overwrite_n_days=self.overwrite_n_days
            )
        )


@workflow_entry_point
class HylImportEnrollmentsIntoMysql(OverwriteMysqlDownstreamMixin, luigi.WrapperTask):
    """Import all breakdowns of enrollment into MySQL."""

    def requires(self):
        enrollment_kwargs = {
            # 'source': self.source,
            # 'interval': self.interval,
            # 'pattern': self.pattern,
            # 'overwrite_n_days': self.overwrite_n_days,
            'overwrite_mysql': self.overwrite_mysql,
        }
        #
        # course_summary_kwargs = dict({
        #     'date': self.date,
        #     'api_root_url': self.api_root_url,
        #     'api_page_size': self.api_page_size,
        #     'enable_course_catalog': self.enable_course_catalog,
        # }, **enrollment_kwargs)
        #
        # course_enrollment_summary_args = dict({
        #     'source': self.source,
        #     'interval': self.interval,
        #     'pattern': self.pattern,
        #     'overwrite_n_days': self.overwrite_n_days,
        #     'overwrite': self.overwrite_hive,
        # })

        yield [
            # TestTask1(**test1_kwargs),
            # TestTask2(**test2_kwargs)
            # The S3 data generated by this job is used by the load_warehouse_bigquery and
            # load_internal_reporting_user_course jobs.
            # CourseEnrollmentSummaryPartitionTask(**course_enrollment_summary_args),
            #
            # EnrollmentByGenderMysqlTask(**enrollment_kwargs),
            # EnrollmentByBirthYearToMysqlTask(**enrollment_kwargs),
            # EnrollmentByEducationLevelMysqlTask(**enrollment_kwargs),
            EnrollmentDailyMysqlTask(**enrollment_kwargs),
            # CourseMetaSummaryEnrollmentIntoMysql(**course_summary_kwargs),
        ]
        # if self.enable_course_catalog:
        #     yield CourseProgramMetadataInsertToMysqlTask(**course_summary_kwargs)


if __name__ == '__main__':
    luigi.run(['HylImportEnrollmentsIntoMysql', '--overwrite-mysql', '--local-scheduler'])
