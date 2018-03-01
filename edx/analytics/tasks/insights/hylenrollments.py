"""Compute metrics related to user enrollments in courses"""

import sys
import datetime
import logging
import luigi.task
from luigi import Task
from luigi.parameter import DateIntervalParameter
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask, IncrementalMysqlInsertTask, get_mysql_query_results
from edx.analytics.tasks.util import eventlog, opaque_key_util
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


class EnrollmentEvent(object):
    """The critical information necessary to process the event in the event stream."""

    def __init__(self, timestamp, event_type, mode):
        self.timestamp = timestamp
        self.datestamp = eventlog.timestamp_to_datestamp(timestamp)
        self.event_type = event_type
        self.mode = mode


class DaysEnrolledForEvents(object):
    """
    Determine which days a user was enrolled in a course given a stream of enrollment events.

    Produces a record for each date from the date the user enrolled in the course for the first time to the end of the
    interval. Note that the user need not have been enrolled in the course for the entire day. These records will have
    the following format:

        datestamp (str): The date the user was enrolled in the course during.
        course_id (str): Identifies the course the user was enrolled in.
        user_id (int): Identifies the user that was enrolled in the course.
        enrolled_at_end (int): 1 if the user was still enrolled in the course at the end of the day.
        change_since_last_day (int): 1 if the user has changed to the enrolled state, -1 if the user has changed
            to the unenrolled state and 0 if the user's enrollment state hasn't changed.

    If the first event in the stream for a user in a course is an unenrollment event, that would indicate that the user
    was enrolled in the course before that moment in time. It is unknown, however, when the user enrolled in the course,
    so we conservatively omit records for the time before that unenrollment event even though it is likely they were
    enrolled in the course for some unknown amount of time before then. Enrollment counts for dates before the
    unenrollment event will be less than the actual value.

    If the last event for a user is an enrollment event, that would indicate that the user was still enrolled in the
    course at the end of the interval, so records are produced from that last enrollment event all the way to the end of
    the interval. If we miss an unenrollment event after this point, it will result in enrollment counts that are
    actually higher than the actual value.

    Both of the above paragraphs describe edge cases that account for the majority of the error that can be observed in
    the results of this analysis.

    Ranges of dates where the user is continuously enrolled will be represented as contiguous records with the first
    record indicating the change (new enrollment), and the last record indicating the unenrollment. It will look
    something like this::

        datestamp,enrolled_at_end,change_since_last_day
        2014-01-01,1,1
        2014-01-02,1,0
        2014-01-03,1,0
        2014-01-04,0,-1
        2014-01-05,0,0

    The above activity indicates that the user enrolled in the course on 2014-01-01 and unenrolled from the course on
    2014-01-04. Records are created for every date after the date when they first enrolled.

    If a user enrolls and unenrolls from a course on the same day, a record will appear that looks like this::

        datestamp,enrolled_at_end,change_since_last_day
        2014-01-01,0,0

    Args:
        course_id (str): Identifies the course the user was enrolled in.
        user_id (int): Identifies the user that was enrolled in the course.
        interval (luigi.date_interval.DateInterval): The interval of time in which these enrollment events took place.
        events (iterable): The enrollment events as produced by the map tasks. This is expected to be an iterable
            structure whose elements are tuples consisting of a timestamp and an event type.

    """

    MODE_UNKNOWN = 'unknown'

    def __init__(self, course_id, user_id, interval, events, increment_counter=None):
        self.course_id = course_id
        self.user_id = user_id
        self.interval = interval
        self.increment_counter = increment_counter

        self.sorted_events = sorted(events)
        # After sorting, we can discard time information since we only care about date transitions.
        self.sorted_events = [
            EnrollmentEvent(timestamp, event_type, mode) for timestamp, event_type, mode in self.sorted_events
        ]
        # Since each event looks ahead to see the time of the next event, insert a dummy event at then end that
        # indicates the end of the requested interval. If the user's last event is an enrollment activation event then
        # they are assumed to be enrolled up until the end of the requested interval. Note that the mapper ensures that
        # no events on or after date_b are included in the analyzed data set.
        self.sorted_events.append(
            EnrollmentEvent(self.interval.date_b.isoformat(), None, None))  # pylint: disable=no-member

        self.first_event = self.sorted_events[0]

        # track the previous state in order to easily detect state changes between days.
        if self.first_event.event_type == DEACTIVATED:
            # First event was an unenrollment event, assume the user was enrolled before that moment in time.
            self.increment_counter("Quality First Event Is Unenrollment")
            log.warning('First event is an unenrollment for user %d in course %s on %s',
                        self.user_id, self.course_id, self.first_event.datestamp)
        elif self.first_event.event_type == MODE_CHANGED:
            self.increment_counter("Quality First Event Is Mode Change")
            log.warning('First event is a mode change for user %d in course %s on %s',
                        self.user_id, self.course_id, self.first_event.datestamp)

        # Before we start processing events, we can assume that their current state is the same as it has been for all
        # time before the first event.
        self.state = self.previous_state = UNENROLLED
        self.mode = self.MODE_UNKNOWN

    def days_enrolled(self):
        """
        A record is yielded for each day during which the user was enrolled in the course.

        Yields:
            tuple: An enrollment record for each day during which the user was enrolled in the course.

        """
        # The last element of the list is a placeholder indicating the end of the interval. Don't process it.
        for index in range(len(self.sorted_events) - 1):
            self.event = self.sorted_events[index]
            self.next_event = self.sorted_events[index + 1]

            self.change_state()

            if self.event.datestamp != self.next_event.datestamp:
                change_since_last_day = self.state - self.previous_state

                # There may be a very wide gap between this event and the next event. If the user is currently
                # enrolled, we can assume they continue to be enrolled at least until the next day we see an event.
                # Emit records for each of those intermediary days. Since the end of the interval is represented by
                # a dummy event at the end of the list of events, it will be represented by self.next_event when
                # processing the last real event in the stream. This allows the records to be produced up to the end
                # of the interval if the last known state was "ENROLLED".
                for datestamp in self.all_dates_between(self.event.datestamp, self.next_event.datestamp):
                    yield self.enrollment_record(
                        datestamp,
                        self.state,
                        change_since_last_day if datestamp == self.event.datestamp else 0,
                        self.mode
                    )

                self.previous_state = self.state

    def all_dates_between(self, start_date_str, end_date_str):
        """
        All dates from the start date up to the end date.

        Yields:
            str: ISO 8601 datestamp for each date from the first date (inclusive) up to the end date (exclusive).

        """
        current_date = self.parse_date_string(start_date_str)
        end_date = self.parse_date_string(end_date_str)

        while current_date < end_date:
            yield current_date.isoformat()
            current_date += datetime.timedelta(days=1)

    def parse_date_string(self, date_str):
        """Efficiently parse an ISO 8601 date stamp into a datetime.date() object."""
        date_parts = [int(p) for p in date_str.split('-')[:3]]
        return datetime.date(*date_parts)

    def enrollment_record(self, datestamp, enrolled_at_end, change_since_last_day, mode_at_end):
        """A complete enrollment record."""
        return (datestamp, self.course_id, self.user_id, enrolled_at_end, change_since_last_day, mode_at_end)

    def change_state(self):
        """Change state when appropriate.

        Note that in spite of our best efforts some events might be lost, causing invalid state transitions.
        """
        if self.state == ENROLLED and self.event.event_type == DEACTIVATED:
            self.state = UNENROLLED
            self.increment_counter("Subset Unenrollment")
        elif self.state == UNENROLLED and self.event.event_type == ACTIVATED:
            self.state = ENROLLED
            self.increment_counter("Subset Enrollment")
        elif self.event.event_type == MODE_CHANGED:
            if self.mode == self.event.mode:
                self.increment_counter("Subset Unchanged")
                self.increment_counter("Subset Unchanged Mode")
            else:
                self.increment_counter("Subset Mode Change")
        else:
            log.warning(
                'No state change for %s event. User %d is already in the requested state for course %s on %s.',
                self.event.event_type, self.user_id, self.course_id, self.event.datestamp
            )
            self.increment_counter("Subset Unchanged")
            self.increment_counter(
                "Subset Unchanged Already {}".format("Enrolled" if self.state == ENROLLED else "Unenrolled"))

        self.mode = self.event.mode


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
    completed = False
    batch_counter_default = 1
    # FILEPATH_PATTERN should match the output files defined by output_path_for_key().
    FILEPATH_PATTERN = '.*?course_enrollment_events_(?P<date>\\d{4}-\\d{2}-\\d{2})'

    # We use warehouse_path to generate the output path, so we make this a non-param.
    output_root = None

    counter_category_name = 'Enrollment Events'

    _counter_dict = {}

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentEventsTask, self).__init__(*args, **kwargs)

    def get_event_row_from_line(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value
        self.incr_counter(self.counter_category_name, 'Inputs with Dates', 1)

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            self.incr_counter(self.counter_category_name, 'Discard Missing Event Type', 1)
            return

        if event_type not in (DEACTIVATED, ACTIVATED, MODE_CHANGED):
            self.incr_counter(self.counter_category_name, 'Discard Non-Enrollment Event Type', 1)
            return

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            log.error("encountered event with bad timestamp: %s", event)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing Timestamp', 1)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing Something', 1)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing Event Data', 1)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing Something', 1)
            return

        course_id = opaque_key_util.normalize_course_id(event_data.get('course_id'))
        if course_id is None or not opaque_key_util.is_valid_course_id(course_id):
            log.error("encountered explicit enrollment event with invalid course_id: %s", event)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing course_id', 1)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing Something', 1)
            return

        user_id = event_data.get('user_id')
        if user_id is None:
            log.error("encountered explicit enrollment event with no user_id: %s", event)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing user_id', 1)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing Something', 1)
            return

        mode = event_data.get('mode')
        if mode is None:
            log.error("encountered explicit enrollment event with no mode: %s", event)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing mode', 1)
            self.incr_counter(self.counter_category_name, 'Discard Enroll Missing Something', 1)
            return

        self.incr_counter(self.counter_category_name, 'Output From Mapper', 1)
        return date_string, (course_id.encode('utf8'), user_id, timestamp, event_type, mode)

    def output(self):
        # for file in files:
        #     for line in file:
        #         row = self.get_event_row_from_line(line)
        #         yield row
        lines = [
            '{"username": "RickyLiu", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "en-US", "time": "2018-01-26T02:49:27.970995+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko", "page": null, "host": "x.shumba.cn", "session": "20c9f73d115de6730e1beb5970fdee99", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 40, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "199.64.7.56", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 40, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Jane_Hu", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "en-US", "time": "2018-01-26T02:55:39.831107+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko", "page": null, "host": "x.shumba.cn", "session": "ee20b8b45687f60485424113bfa1f1e1", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 42, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "116.6.51.15", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 42, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Jane_Hu", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "en-US", "time": "2018-01-26T02:59:12.651847+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko", "page": null, "host": "x.shumba.cn", "session": "ee20b8b45687f60485424113bfa1f1e1", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+QTM101+2016_08/about", "context": {"user_id": 42, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+QTM101+2016_08", "path": "/change_enrollment"}, "ip": "116.6.51.15", "event": {"course_id": "course-v1:SHUMBAX+QTM101+2016_08", "user_id": 42, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Gregwang", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-cn", "time": "2018-01-26T03:23:01.150047+00:00", "agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_4 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Mobile/13G35 MicroMessenger/6.3.23 NetType/3G Language/zh_CN", "page": null, "host": "x.shumba.cn", "session": "ba2c2a7f7a260b16cbc42153b13f0f9e", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 47, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "114.81.254.175", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 47, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "LuBo", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN", "time": "2018-01-26T03:23:08.298356+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.10240", "page": null, "host": "x.shumba.cn", "session": "7f6ed05d6b54ebff37b119f7dc45c355", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 48, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "112.232.210.121", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 48, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Paula", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8", "time": "2018-01-26T03:37:00.247880+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5debeae423e91faee31726f92e2a07f8", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 50, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "58.211.28.242", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 50, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Peter_Wang", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8", "time": "2018-01-26T03:40:24.379364+00:00", "agent": "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "812a8d30a86720039a9dad7b0ef26889", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 51, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "112.54.80.84", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 51, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Sindy", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8", "time": "2018-01-26T03:40:52.111757+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "03e478d9b2f97a77ff8d2f205eca2520", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+QTM101+2016_08/instructor", "context": {"course_user_tags": {}, "user_id": 13, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+QTM101+2016_08", "path": "/courses/course-v1:SHUMBAX+QTM101+2016_08/instructor/api/students_update_enrollment"}, "ip": "58.198.74.180", "event": {"course_id": "course-v1:SHUMBAX+QTM101+2016_08", "user_id": 28, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Paula", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8", "time": "2018-01-26T03:42:16.217815+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5debeae423e91faee31726f92e2a07f8", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+QTM101+2016_08/about", "context": {"user_id": 50, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+QTM101+2016_08", "path": "/change_enrollment"}, "ip": "58.211.28.242", "event": {"course_id": "course-v1:SHUMBAX+QTM101+2016_08", "user_id": 50, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "staff", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-cn", "time": "2018-01-26T04:08:06.627615+00:00", "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7", "page": null, "host": "x.shumba.cn", "session": "3369733b774c79c603808956a7ddab0c", "referer": "https://x.shumba.cn:28010/course_team/course-v1:SHUMBAX+ADM101+2016_8", "context": {"user_id": 5, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/course_team/course-v1:SHUMBAX+ADM101+2016_8/ericqian@126.com"}, "ip": "1.198.35.211", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 10, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "Cody", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-Hans-CN,zh-Hans;q=0.8,en-GB;q=0.5,en;q=0.3", "time": "2018-01-26T04:09:34.244751+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586", "page": null, "host": "x.shumba.cn", "session": "ae2475d1e87f91995e2cdb719352820d", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 55, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "61.164.138.232", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 55, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "cheney", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8", "time": "2018-01-26T04:12:58.985241+00:00", "agent": "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36 QQBrowser/9.3.6581.400", "page": null, "host": "x.shumba.cn", "session": "520c69c5853dde62d45e227ba0ee16cb", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 32, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "59.63.170.5", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 32, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "kuangxu", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "en-US", "time": "2018-02-24T15:38:28.177968+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko", "page": null, "host": "x.shumba.cn", "session": "0f79c9fbf83ec89ab49bd9d607318c63", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+QTM101+2016_08/about", "context": {"user_id": 111, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+QTM101+2016_08", "path": "/change_enrollment"}, "ip": "202.79.203.83", "event": {"course_id": "course-v1:SHUMBAX+QTM101+2016_08", "user_id": 111, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "LizzaDu", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-cn", "time": "2018-02-24T15:38:43.887522+00:00", "agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 9_3_2 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13F69 Safari/601.1", "page": null, "host": "x.shumba.cn", "session": "9d56e70e00e5ca8591f0ac8db24062ab", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+QTM101+2016_08/about", "context": {"user_id": 36, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+QTM101+2016_08", "path": "/change_enrollment"}, "ip": "116.230.196.113", "event": {"course_id": "course-v1:SHUMBAX+QTM101+2016_08", "user_id": 36, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "kuangxu", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "en-US", "time": "2018-02-24T15:47:11.010232+00:00", "agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko", "page": null, "host": "x.shumba.cn", "session": "0f79c9fbf83ec89ab49bd9d607318c63", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+ADM101+2016_8/about", "context": {"user_id": 111, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+ADM101+2016_8", "path": "/change_enrollment"}, "ip": "202.79.203.83", "event": {"course_id": "course-v1:SHUMBAX+ADM101+2016_8", "user_id": 111, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-cn", "time": "2018-03-03T16:17:06.558264+00:00", "agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14", "page": null, "host": "x.shumba.cn", "session": "337e33ff57e124f904880a6ef26c8ba5", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU804+2016_T2/about", "context": {"user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU804+2016_T2", "path": "/change_enrollment"}, "ip": "1.198.34.39", "event": {"course_id": "course-v1:SHUMBAX+SHU804+2016_T2", "user_id": 10, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-Hans-CN;q=1.0,en-CN;q=0.9,zh-Hant-CN;q=0.8", "time": "2018-03-04T12:54:02.918330+00:00", "agent": "edX/com.editech.mba.app.shumbax (2.4.0.1; OS Version 10.1.1 (Build 14B100))", "page": null, "host": "x.shumba.cn", "session": "9dacd8eee6f3702a1f20b2e44e846cd7", "referer": "", "context": {"user_id": null, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU807+2016_T2", "path": "/api/enrollment/v1/enrollment"}, "ip": "1.198.35.235", "event": {"course_id": "course-v1:SHUMBAX+SHU807+2016_T2", "user_id": 86, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-04T13:17:34.084372+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "b6eb6b4f310b701f2df9a7f7328bb497", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU807+2016_T2/about", "context": {"user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU807+2016_T2", "path": "/change_enrollment"}, "ip": "1.198.35.235", "event": {"course_id": "course-v1:SHUMBAX+SHU807+2016_T2", "user_id": 10, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-04T13:19:49.118376+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "81659757387a13671d21db355790df25", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU804+2016_T2/instructor", "context": {"course_user_tags": {}, "user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU804+2016_T2", "path": "/courses/course-v1:SHUMBAX+SHU804+2016_T2/instructor/api/students_update_enrollment"}, "ip": "1.198.35.235", "event": {"course_id": "course-v1:SHUMBAX+SHU804+2016_T2", "user_id": 86, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ecpolit", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-05T03:10:29.105299+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "3e908817fe8413a31b8bb12916971cda", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU804+2016_T2/about", "context": {"user_id": 114, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU804+2016_T2", "path": "/change_enrollment"}, "ip": "1.198.34.18", "event": {"course_id": "course-v1:SHUMBAX+SHU804+2016_T2", "user_id": 114, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-05T03:14:04.859630+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5aecb29254ac7c451be5c98aa8c8e59d", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU807+2016_T2/instructor", "context": {"course_user_tags": {}, "user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU807+2016_T2", "path": "/courses/course-v1:SHUMBAX+SHU807+2016_T2/instructor/api/students_update_enrollment"}, "ip": "1.198.34.18", "event": {"course_id": "course-v1:SHUMBAX+SHU807+2016_T2", "user_id": 114, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-05T03:14:16.240637+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5aecb29254ac7c451be5c98aa8c8e59d", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU805+2016_T2/about", "context": {"user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU805+2016_T2", "path": "/change_enrollment"}, "ip": "1.198.34.18", "event": {"course_id": "course-v1:SHUMBAX+SHU805+2016_T2", "user_id": 10, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-05T03:14:35.941904+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5aecb29254ac7c451be5c98aa8c8e59d", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU805+2016_T2/instructor", "context": {"course_user_tags": {}, "user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU805+2016_T2", "path": "/courses/course-v1:SHUMBAX+SHU805+2016_T2/instructor/api/students_update_enrollment"}, "ip": "1.198.34.18", "event": {"course_id": "course-v1:SHUMBAX+SHU805+2016_T2", "user_id": 114, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-05T03:14:57.113661+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5aecb29254ac7c451be5c98aa8c8e59d", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU806+2016_T2/about", "context": {"user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU806+2016_T2", "path": "/change_enrollment"}, "ip": "1.198.34.18", "event": {"course_id": "course-v1:SHUMBAX+SHU806+2016_T2", "user_id": 10, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-05T03:15:40.849946+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5aecb29254ac7c451be5c98aa8c8e59d", "referer": "https://x.shumba.cn/courses/course-v1:SHUMBAX+SHU806+2016_T2/instructor", "context": {"course_user_tags": {}, "user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+SHU806+2016_T2", "path": "/courses/course-v1:SHUMBAX+SHU806+2016_T2/instructor/api/students_update_enrollment"}, "ip": "1.198.34.18", "event": {"course_id": "course-v1:SHUMBAX+SHU806+2016_T2", "user_id": 114, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"}',
            '{"username": "ericqian", "event_source": "server", "name": "edx.course.enrollment.activated", "accept_language": "zh-CN,zh;q=0.8,en;q=0.6", "time": "2018-03-05T03:15:54.368907+00:00", "agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "page": null, "host": "x.shumba.cn", "session": "5aecb29254ac7c451be5c98aa8c8e59d", "referer": "https://exmail.qq.com/cgi-bin/mail_spam?action=check_link&url=https%3A//x.shumba.cn/activate/7d8446d49b074da7a3139e75ecbf5f55&mailid=ZC0905-H3HtIa3g4yQ5TGQ8u_ifW6n&spam=0&r=0.7339119604091979", "context": {"user_id": 10, "org_id": "SHUMBAX", "course_id": "course-v1:SHUMBAX+QTM101+2016_08", "path": "/activate/7d8446d49b074da7a3139e75ecbf5f55"}, "ip": "1.198.34.18", "event": {"course_id": "course-v1:SHUMBAX+QTM101+2016_08", "user_id": 114, "mode": "audit"}, "event_type": "edx.course.enrollment.activated"'
        ]
        raw_events = {}
        for line in lines:
            event_row = self.get_event_row_from_line(line)
            if not event_row:
                continue
            date_string, event = event_row
            log.info("row: date_string = {} event = {}".format(date_string, event))
            if date_string in raw_events:
                raw_events[date_string].append(event)
            else:
                raw_events[date_string] = [event]
        log.debug("raw_events: {}".format(raw_events))

        increment_counter = lambda counter_name: self.incr_counter(self.counter_category_name, counter_name, 1)

        for date_string in raw_events.iterkeys():
            day_events = raw_events[date_string]
            log.debug("day_events: raw_events[{}] = {}".format(date_string, day_events))
            user_events = {}
            # rows = []
            for day_event_raw in day_events:
                (course_id, user_id, timestamp, event_type, mode) = day_event_raw
                k = (course_id, user_id)
                v = (timestamp, event_type, mode)
                if k in user_events:
                    user_events[k].append(v)
                else:
                    user_events[k] = [v]
            for k in user_events.iterkeys():
                v = user_events[k]
                log.debug("user_events: user_events[{}] = {}".format(k, v))
                course_id, user_id = k
                event_stream_processor = DaysEnrolledForEvents(course_id, user_id, self.interval, v,
                                                               increment_counter)
                for day_enrolled_record in event_stream_processor.days_enrolled():
                    # rows.append(day_enrolled_record)
                    yield day_enrolled_record
        # log.debug("rows = {}".format(rows))
        # for row in rows:
        #     yield row
        # rows = [
        #     ('2018-02-04', 'courseid1', '789789', True, True, '1'),
        #     ('2018-02-02', 'courseid1', '123123', True, True, '1'),
        #     ('2018-02-02', 'courseid1', '123123', True, True, '1'),
        #     ('2018-02-03', 'courseid1', '567567', True, True, '1'),
        #     ('2018-02-03', 'courseid1', '567567', True, True, '1'),
        #     ('2018-02-03', 'courseid1', '567567', True, True, '1'),
        #     ('2018-02-03', 'courseid1', '567567', True, True, '1'),
        #     ('2018-02-04', 'courseid1', '789789', True, True, '1'),
        #     ('2018-02-04', 'courseid1', '789789', True, True, '1'),
        #     ('2018-02-04', 'courseid1', '789789', True, True, '1'),
        #     ('2018-02-04', 'courseid1', '789789', True, True, '1'),
        #     ('2018-02-04', 'courseid1', '789789', True, True, '1')
        # ]

    def _incr_counter(self, *args):
        """ Increments a Hadoop counter

        Note that this seems to be a bit slow, ~1 ms. Don't overuse this function by updating very frequently.
        """
        if len(args) == 2:
            # backwards compatibility with existing hadoop jobs
            group_name, count = args
            print >> sys.stderr, 'reporter:counter:%s,%s' % (group_name, count)
        else:
            group, name, count = args
            print >> sys.stderr, 'reporter:counter:%s,%s,%s' % (group, name, count)

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

    def complete(self):
        return self.completed
        # return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def init_local(self):
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def run(self):
        log.info('test-run')
        self.init_local()
        super(CourseEnrollmentEventsTask, self).run()
        if not self.completed:
            self.completed = True

    def requires(self):
        requires = super(CourseEnrollmentEventsTask, self).requires()
        if isinstance(requires, luigi.Task):
            yield requires
        else:
            for requirement in requires:
                yield requirement


class CourseEnrollmentTask(OverwriteMysqlDownstreamMixin, CourseEnrollmentDownstreamMixin, IncrementalMysqlInsertTask):
    """Produce a data set that shows which days each user was enrolled in each course."""

    overwrite = None

    def __init__(self, *args, **kwargs):
        super(CourseEnrollmentTask, self).__init__(*args, **kwargs)
        self.overwrite = self.overwrite_mysql
        self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def table(self):  # pragma: no cover
        return 'course_enrollment'

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
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
            return """`date` >= '{}' AND `date` <= '{}'""".format(self.overwrite_from_date.isoformat(),
                                                                  self.interval.date_b.isoformat())
        else:
            return None
        # return """`date=`=`query_date`""".format(query_date=self.query_date)

    def requires_local(self):
        if self.overwrite_n_days == 0:
            return []

        overwrite_interval = DateIntervalParameter().parse('{}-{}'.format(
            self.overwrite_from_date,
            self.interval.date_b
        ))

        return CourseEnrollmentEventsTask(
            interval=overwrite_interval,
            source=self.source,
            pattern=self.pattern
        )

    def requires(self):
        for requirement in super(CourseEnrollmentTask, self).requires().itervalues():
            yield requirement

        requires_local = self.requires_local()
        if isinstance(requires_local, luigi.Task):
            yield requires_local


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
        self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def table(self):  # pragma: no cover
        return 'course_enrollment_daily'

    @property
    def insert_query(self):
        """The query builder that controls the structure and fields inserted into the new table."""
        if self.overwrite:
            from_date = self.overwrite_from_date
        else:
            from_date = self.interval.date_a

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
        """.format(from_date.isoformat(), self.interval.date_b.isoformat())
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
            return """`date` >= '{}' AND `date` <= '{}'""".format(self.overwrite_from_date.isoformat(),
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
class HylImportEnrollmentsIntoMysql(CourseEnrollmentDownstreamMixin, OverwriteMysqlDownstreamMixin, luigi.WrapperTask):
    """Import all breakdowns of enrollment into MySQL."""

    def requires(self):
        enrollment_kwargs = {
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite_n_days': self.overwrite_n_days,
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
