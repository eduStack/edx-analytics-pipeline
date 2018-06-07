"""Tasks for aggregating statistics about video viewing."""
import datetime
import json
import gzip
import logging
import math
import re
import textwrap
import urllib
from collections import namedtuple
import pandas as pd
import ciso8601
import luigi
from luigi import configuration
from luigi.hive import HiveQueryTask
from luigi.parameter import DateIntervalParameter

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask, IncrementalMysqlInsertTask, get_mysql_query_results
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import (
    BareHiveTableTask, HivePartition, HivePartitionTask, HiveTableTask, WarehouseMixin, hive_database_name
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import IntegerField, Record, StringField
from edx.analytics.tasks.util.url import UncheckedExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

VIDEO_CODES = frozenset([
    'html5',
    'mobile',
    'hls',
])
VIDEO_PLAYED = 'play_video'
VIDEO_PAUSED = 'pause_video'
VIDEO_SEEK = 'seek_video'
VIDEO_STOPPED = 'stop_video'
VIDEO_EVENT_TYPES = frozenset([
    VIDEO_PLAYED,
    VIDEO_PAUSED,
    VIDEO_SEEK,
    VIDEO_STOPPED,
])
# Any event that contains the events above must also contain this string.
VIDEO_EVENT_MINIMUM_STRING = '_video'

VIDEO_MAXIMUM_DURATION = 3600 * 10.0  # 10 hours, converted to seconds
VIDEO_UNKNOWN_DURATION = -1
VIDEO_VIEWING_SECONDS_PER_SEGMENT = 5
VIDEO_VIEWING_MINIMUM_LENGTH = 0.25  # seconds

VideoViewing = namedtuple('VideoViewing', [  # pylint: disable=invalid-name
    'start_timestamp', 'course_id', 'encoded_module_id', 'start_offset', 'video_duration'])


class VideoTimelineRecord(Record):
    """
    Video Segment Information used to populate the video_timeline table
    """

    pipeline_video_id = StringField(length=255,
                                    nullable=False,
                                    description='A concatenation of the course_id and the HTML encoded ID of the video.'
                                                ' Intended to uniquely identify an instance of a video in a particular '
                                                'course. Note that ideally we would use an XBlock usage_id here, but '
                                                'it isn\'t present on the legacy events.')
    segment = IntegerField(description='An integer representing the rank of the segment within the video. 0 is the '
                                       'first segment, 1 is the second etc. Note that this does not specify the length '
                                       'of the segment.')
    num_users = IntegerField(description='The number of unique users who watched any part of this segment of the '
                                         'video.')
    num_views = IntegerField(description='The total number of times any part of the segment was viewed, regardless of '
                                         'who was watching it.')


class VideoSegmentSummaryRecord(Record):
    """
    Video Segment Summary Information used to populate the video table
    """

    pipeline_video_id = StringField(length=255,
                                    nullable=False,
                                    description='A concatenation of the course_id and the HTML encoded ID of the video.'
                                                ' Intended to uniquely identify an instance of a video in a particular '
                                                'course. Note that ideally we would use an XBlock usage_id here, but '
                                                'it isn\'t present on the legacy events.')
    course_id = StringField(length=255,
                            nullable=False,
                            description='Course the video was displayed in. This is an opaque key serialized to '
                                        'a string.')
    encoded_module_id = StringField(length=255,
                                    nullable=False,
                                    description='This is the HTML encoded module ID for the video. Ideally this would '
                                                'be an XBlock usage_id, but that data is not present on legacy events.')
    duration = IntegerField(description='The video length in seconds. This can be inferred for some videos. We don\'t '
                                        'have reliable metadata for the length of videos in the source data.')
    segment_length = IntegerField(description='The length of each segment, in seconds.')
    users_at_start = IntegerField(description='The number of users who watched the first segment of the video.')
    users_at_end = IntegerField(description='The number of users who watched the end of the video. Note that this is '
                                            'not the number of users who watched the last segment of the video.')
    total_viewed_seconds = IntegerField(description='The total number of seconds viewed by all users across all '
                                                    'segments.')


class VideoSegmentDetailRecord(Record):
    """
    Video Segment Usage Detail
    """

    pipeline_video_id = StringField(length=255,
                                    nullable=False,
                                    description='A concatenation of the course_id and the HTML encoded ID of the video.'
                                                ' Intended to uniquely identify an instance of a video in a particular '
                                                'course. Note that ideally we would use an XBlock usage_id here, but '
                                                'it isn\'t present on the legacy events.')
    course_id = StringField(length=255,
                            nullable=False,
                            description='Course the video was displayed in. This is an opaque key serialized to '
                                        'a string.')
    encoded_module_id = StringField(length=255,
                                    nullable=False,
                                    description='This is the HTML encoded module ID for the video. Ideally this would '
                                                'be an XBlock usage_id, but that data is not present on legacy events.')
    duration = IntegerField(description='The video length in seconds. This can be inferred for some videos. We don\'t '
                                        'have reliable metadata for the length of videos in the source data.')
    segment_length = IntegerField(description='The length of each segment, in seconds.')
    users_at_start = IntegerField(description='The number of users who watched the first segment of the video.')
    users_at_end = IntegerField(description='The number of users who watched the end of the video. Note that this is '
                                            'not the number of users who watched the last segment of the video.')
    segment = IntegerField(description='An integer representing the rank of the segment within the video. 0 is the '
                                       'first segment, 1 is the second etc. Note that this does not specify the length '
                                       'of the segment.')
    num_users = IntegerField(description='The number of unique users who watched any part of this segment of the '
                                         'video.')
    num_views = IntegerField(description='The total number of times any part of the segment was viewed, regardless of '
                                         'who was watching it.')


class VideoTableDownstreamMixin(EventLogSelectionDownstreamMixin):
    """All parameters needed to run the VideoUsageTask and its required tasks."""
    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'videos', 'name': 'overwrite_n_days'},
        significant=False,
        default=3,
    )


class VideoUsageTask(VideoTableDownstreamMixin, EventLogSelectionMixin, luigi.Task):
    completed = False
    # Cache for storing duration values fetched from Youtube.
    # Persist this across calls to the reducer.
    video_durations = {}

    counter_category_name = 'Video Events'
    _counter_dict = {}
    batch_counter_default = 1

    def complete(self):
        return self.completed

    def _check_time_offset(self, time_value, line):
        """Check that time can be converted to a float, and has a reasonable value."""
        if time_value is None:
            log.warn('Video with missing time_offset value: {0}'.format(line))
            return None

        try:
            time_value = float(time_value)
        except ValueError:
            log.warn('Video event with invalid time-offset value: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Invalid Time-Offset Value', 1)
            return None
        except TypeError:
            log.warn('Video event with invalid time-offset type: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Invalid Time-Offset Type', 1)
            return None

        # Some events have ridiculous (and dangerous) values for time.
        if time_value > VIDEO_MAXIMUM_DURATION:
            log.warn('Video event with huge time-offset value: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Huge Time-Offset Value', 1)
            return None

        if time_value < 0.0:
            log.warn('Video event with negative time-offset value: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Negative Time-Offset Value', 1)
            return None

        # We must screen out 'nan' and 'inf' values, as they do not "round-trip".
        # In Luigi, the mapper calls repr() and the reducer calls eval(), but
        # eval(repr(float('nan'))) throws a NameError rather than returning float('nan').
        if math.isnan(time_value) or math.isinf(time_value):
            log.warn('Video event with nan or inf time-offset value: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Nan-Inf Time-Offset Value', 1)
            return None

        return time_value

    def get_event_and_date_string(self, line):
        """Default mapper implementation, that always outputs the log line, but with a configurable key."""
        event = eventlog.parse_json_event(line)
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

    def get_event_row_from_line(self, line):
        # Add a filter here to permit quicker rejection of unrelated events.
        if VIDEO_EVENT_MINIMUM_STRING not in line:
            # self.incr_counter(self.counter_category_name, 'Discard Missing Video String', 1)
            return

        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value
        # self.incr_counter(self.counter_category_name, 'Inputs with Dates', 1)

        event_type = event.get('event_type')
        if event_type is None:
            log.error("encountered event with no event_type: %s", event)
            self.incr_counter(self.counter_category_name, 'Discard Missing Event Type', 1)
            return

        if event_type not in VIDEO_EVENT_TYPES:
            # self.incr_counter(self.counter_category_name, 'Discard Non-Video Event Type', 1)
            return

        # self.incr_counter(self.counter_category_name, 'Input Video Events', 1)

        # This has already been checked when getting the event, so just fetch the value.
        timestamp = eventlog.get_event_time_string(event)

        # Strip username to remove trailing newlines that mess up Luigi.
        username = event.get('username', '').strip()
        if not username:
            log.error("Video event without username: %s", event)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing username', 1)
            return

        course_id = eventlog.get_course_id(event)
        if course_id is None:
            log.warn('Video event without valid course_id: {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing course_id', 1)
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            # This should already have been logged.
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Event Data', 1)
            return

        encoded_module_id = event_data.get('id', '').strip()  # we have seen id values with leading newline
        if not encoded_module_id:
            log.warn('Video event without valid encoded_module_id (id): {0}'.format(line))
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
            # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing encoded_module_id', 1)
            return

        video_duration = event_data.get('duration', VIDEO_UNKNOWN_DURATION)
        if not video_duration:
            # events may have a 'duration' value of null, so use the same default for those as well.
            video_duration = VIDEO_UNKNOWN_DURATION

        # self.incr_counter(self.counter_category_name, 'Video Events Before Time Check', 1)

        current_time = None
        old_time = None
        youtube_id = None
        if event_type == VIDEO_PLAYED:
            code = event_data.get('code')
            if code not in VIDEO_CODES:
                youtube_id = code
            current_time = self._check_time_offset(event_data.get('currentTime'), line)
            if current_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Play', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Play', 1)
        elif event_type == VIDEO_PAUSED:
            # Pause events may have a missing currentTime value if video is paused at the beginning,
            # so provide a default of zero.
            current_time = self._check_time_offset(event_data.get('currentTime', 0), line)
            if current_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Pause', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Pause', 1)
        elif event_type == VIDEO_SEEK:
            current_time = self._check_time_offset(event_data.get('new_time'), line)
            old_time = self._check_time_offset(event_data.get('old_time'), line)
            if current_time is None or old_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Seek', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Seek', 1)
        elif event_type == VIDEO_STOPPED:
            current_time = self._check_time_offset(event_data.get('currentTime'), line)
            if current_time is None:
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Something', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Video Missing Time From Stop', 1)
                return
            # Slow: self.incr_counter(self.counter_category_name, 'Subset Stop', 1)

        if youtube_id is not None:
            youtube_id = youtube_id.encode('utf8')

        # self.incr_counter(self.counter_category_name, 'Output Video Events from Mapper', 1)
        return (
            username.encode('utf8'), course_id.encode('utf8'), encoded_module_id.encode('utf8'),
            timestamp, event_type, current_time, old_time, youtube_id, video_duration
        )

    def get_raw_events_from_log_file(self, input_file):
        raw_events = []
        for line in input_file:
            event_row = self.get_event_row_from_line(line)
            if not event_row:
                continue
            # (
            #     username, course_id, encoded_module_id,
            #     timestamp, event_type, current_time, old_time, youtube_id, video_duration
            # ) = event_row
            # # reformat data for aggregation
            # event_row = (
            #     (course_id, encoded_module_id),
            #     (timestamp, event_type, current_time, old_time, youtube_id, video_duration)
            # )
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

        columns = ['username', 'course_id', 'encoded_module_id', 'timestamp', 'event_type', 'current_time', 'old_time',
                   'youtube_id', 'video_duration']

        df = pd.DataFrame(data=raw_events, columns=columns)

        for (username, course_id, encoded_module_id), group in df.groupby(
                ['username', 'course_id', 'encoded_module_id']):
            values = group[
                ['timestamp', 'event_type', 'current_time', 'old_time', 'youtube_id', 'video_duration']].get_values()
            sorted_events = sorted(values, key=lambda x: x[0])
            key = (username, course_id, encoded_module_id)
            # When a user seeks forward while the video is playing, it is common to see an incorrect value for currentTime
            # in the play event emitted after the seek. The expected behavior here is play->seek->play with the second
            # play event being emitted almost immediately after the seek. This second play event should record the
            # currentTime after the seek is complete, not the currentTime before the seek started, however it often
            # records the time before the seek started. We choose to trust the seek_video event in these cases and the time
            # it claims to have moved the position to.
            last_viewing_end_event = None
            viewing = None
            for event in sorted_events:
                # self.incr_counter(self.counter_category_name, 'Input User_course_video events', 1)

                timestamp, event_type, current_time, old_time, youtube_id, duration = event
                parsed_timestamp = ciso8601.parse_datetime(timestamp)
                if current_time is not None:
                    current_time = float(current_time)
                if old_time is not None:
                    old_time = float(old_time)

                def start_viewing():
                    """Returns a 'viewing' object representing the point where a video began to be played."""
                    # self.incr_counter(self.counter_category_name, 'Viewing Start', 1)

                    video_duration = duration
                    # video_duration is set to VIDEO_UNKNOWN_DURATION only when duration is not present in
                    # a video event, In that case fetch duration using youtube API if video is from youtube.
                    if video_duration == VIDEO_UNKNOWN_DURATION and youtube_id:
                        # self.incr_counter(self.counter_category_name, 'Viewing Start with Video Id', 1)
                        video_duration = self.video_durations.get(youtube_id)
                        if not video_duration:
                            video_duration = self.get_video_duration(youtube_id)
                            # Duration might still be unknown, but just store it.
                            self.video_durations[youtube_id] = video_duration

                    if last_viewing_end_event is not None and last_viewing_end_event[1] == VIDEO_SEEK:
                        start_offset = last_viewing_end_event[2]
                        # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing Start With Offset From Preceding Seek', 1)
                    else:
                        start_offset = current_time
                        # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing Start With Offset From Current Play', 1)
                    return VideoViewing(
                        start_timestamp=parsed_timestamp,
                        course_id=course_id,
                        encoded_module_id=encoded_module_id,
                        start_offset=start_offset,
                        video_duration=video_duration
                    )

                def end_viewing(end_time):
                    """Returns a "viewing" record by combining the end_time with the current 'viewing' object."""

                    # Check that end_time is within the bounds of the duration.
                    # Note that duration may be an int, and end_time may be a float,
                    # so just add +1 to avoid these round-off errors (instead of actually checking types).
                    # Slow: self.incr_counter(self.counter_category_name, 'Viewing End', 1)

                    if viewing.video_duration != VIDEO_UNKNOWN_DURATION and end_time > (viewing.video_duration + 1):
                        log.error('End time of viewing past end of video.\nViewing Start: %r\nEvent: %r\nKey:%r',
                                  viewing, event, key)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End Time Past End Of Video', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                        return None

                    if end_time < viewing.start_offset:
                        log.error('End time is before the start time.\nViewing Start: %r\nEvent: %r\nKey:%r',
                                  viewing, event, key)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End Time Before Start Time', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                        return None

                    if (end_time - viewing.start_offset) < VIDEO_VIEWING_MINIMUM_LENGTH:
                        log.error('Viewing too short and discarded.\nViewing Start: %r\nEvent: %r\nKey:%r',
                                  viewing, event, key)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End Time Too Short', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                        return None

                    return (
                        username,
                        viewing.course_id,
                        viewing.encoded_module_id,
                        viewing.video_duration,
                        viewing.start_timestamp.isoformat(),
                        viewing.start_offset,
                        end_time,
                        event_type,
                    )

                if event_type == VIDEO_PLAYED:
                    if viewing:
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start On Successive Play', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                        pass
                    viewing = start_viewing()
                    last_viewing_end_event = None
                elif viewing:
                    viewing_end_time = None
                    if event_type in (VIDEO_PAUSED, VIDEO_STOPPED):
                        # play -> pause or play -> stop
                        viewing_end_time = current_time
                        # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing End By Stop Or Pause', 1)
                    elif event_type == VIDEO_SEEK:
                        # play -> seek
                        viewing_end_time = old_time
                        # Slow: self.incr_counter(self.counter_category_name, 'Subset Viewing End By Seek', 1)
                    else:
                        log.error('Unexpected event in viewing.\nViewing Start: %r\nEvent: %r\nKey:%r', viewing, event,
                                  key)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard End Viewing Unexpected Event', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing End', 1)
                        # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                    if viewing_end_time is not None:
                        record = end_viewing(viewing_end_time)
                        if record:
                            # Slow: self.incr_counter(self.counter_category_name, 'Output Viewing', 1)
                            # yield generate_usage_record(record)
                            yield record
                        # Throw away the viewing even if it didn't yield a valid record. We assume that this is malformed
                        # data and untrustworthy.
                        viewing = None
                        last_viewing_end_event = event
                else:
                    # This is a non-play video event outside of a viewing.  It is probably too frequent to be logged.
                    # Slow: self.incr_counter(self.counter_category_name, 'Discard Event Outside Of Viewing', 1)
                    pass

            if viewing is not None:
                # This happens too often!  Comment out for now...
                # log.error('Unexpected viewing started with no matching end.\n'
                #           'Viewing Start: %r\nLast Event: %r\nKey:%r', viewing, last_viewing_end_event, key)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start With No Matching End', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing Start', 1)
                # Slow: self.incr_counter(self.counter_category_name, 'Discard Viewing', 1)
                pass

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

    def init_local(self):
        # Providing an api_key is optional.
        self.api_key = configuration.get_config().get('google', 'api_key', None)
        # Reset this (mostly for the sake of tests).
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    def run(self):
        log.info('test VideoUsageTask')
        self.init_local()
        super(VideoUsageTask, self).run()
        if self.completed is False:
            self.completed = True

    def requires(self):
        requires = super(VideoUsageTask, self).requires()
        if isinstance(requires, luigi.Task):
            yield requires

    def get_video_duration(self, youtube_id):
        """
        For youtube videos, queries Google API for video duration information.

        This returns an "unknown" duration flag if no API key has been defined, or if the query fails.
        """
        duration = VIDEO_UNKNOWN_DURATION
        if self.api_key is None:
            return duration

        # Slow: self.incr_counter(self.counter_category_name, 'Subset Calls to Youtube API', 1)
        video_file = None
        try:
            video_url = "https://www.googleapis.com/youtube/v3/videos?id={0}&part=contentDetails&key={1}".format(
                youtube_id, self.api_key
            )
            video_file = urllib.urlopen(video_url)
            content = json.load(video_file)
            items = content.get('items', [])
            if len(items) > 0:
                duration_str = items[0].get(
                    'contentDetails', {'duration': 'MISSING_CONTENTDETAILS'}
                ).get('duration', 'MISSING_DURATION')
                matcher = re.match(r'PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?', duration_str)
                if not matcher:
                    log.error('Unable to parse duration returned for video %s: %s', youtube_id, duration_str)
                    # Slow: self.incr_counter(self.counter_category_name, 'Quality Unparseable Response From Youtube API', 1)
                else:
                    duration_secs = int(matcher.group('hours') or 0) * 3600
                    duration_secs += int(matcher.group('minutes') or 0) * 60
                    duration_secs += int(matcher.group('seconds') or 0)
                    duration = duration_secs
                    # Slow: self.incr_counter(self.counter_category_name, 'Subset Calls to Youtube API Succeeding', 1)
            else:
                log.error('Unable to find items in response to duration request for youtube video: %s', youtube_id)
                # Slow: self.incr_counter(self.counter_category_name, 'Quality No Items In Response From Youtube API', 1)
        except Exception:  # pylint: disable=broad-except
            log.exception("Unrecognized response from Youtube API")
            # Slow: self.incr_counter(self.counter_category_name, 'Quality Unrecognized Response From Youtube API', 1)
        finally:
            if video_file is not None:
                video_file.close()

        return duration


class VideoTimelineDataTask(VideoTableDownstreamMixin, IncrementalMysqlInsertTask):
    dropoff_threshold = luigi.FloatParameter(config_path={'section': 'videos', 'name': 'dropoff_threshold'})

    def __init__(self, *args, **kwargs):
        super(VideoTimelineDataTask, self).__init__(*args, **kwargs)

        overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)
        self.overwrite_interval = DateIntervalParameter().parse('{}-{}'.format(
            overwrite_from_date,
            self.interval.date_b
        ))

    @property
    def table(self):  # pragma: no cover
        return 'video_usage'

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def columns(self):  # pragma: no cover
        return VideoSegmentDetailRecord.get_sql_schema()

    def snap_to_last_segment_boundary(self, second):
        """Maps a time_offset to a segment index."""
        return (int(second) / VIDEO_VIEWING_SECONDS_PER_SEGMENT)

    def get_final_segment(self, usage_map):
        """
        Identifies the final segment by looking for a sharp drop in number of users per segment.
        Needed as some events appear after the actual end of videos.
        """
        final_segment = last_segment = max(usage_map.keys())
        last_segment_num_users = len(usage_map[last_segment]['users'])
        for segment in sorted(usage_map.keys(), reverse=True)[1:]:
            stats = usage_map[segment]
            current_segment_num_users = len(stats.get('users', []))
            if last_segment_num_users <= current_segment_num_users * self.dropoff_threshold:
                final_segment = segment
                break
            last_segment_num_users = current_segment_num_users
            last_segment = segment
        return final_segment

    def complete_end_segment(self, duration):
        """
        Calculates a complete end segment(if the user has watched till this segment,
        we consider the user to have watched the complete video) by cutting off the minimum of
        30 seconds and 5% of duration. Needed to cut off video credits etc.
        """
        complete_end_time = max(duration - 30, duration * 0.95)
        return self.snap_to_last_segment_boundary(complete_end_time)

    def rows(self):
        require = self.requires_local()
        if not require:
            pass
        events = []
        for raw_event in require.output():
            events.append(raw_event)

        log.info('events = {}'.format(events))
        columns = ['username', 'course_id', 'encoded_module_id', 'video_duration', 'start_timestamp', 'start_offset',
                   'end_time', 'event_type']

        df = pd.DataFrame(data=events, columns=columns)

        for (course_id, encoded_module_id), group in df.groupby(
                ['course_id', 'encoded_module_id']):
            values = group[
                ['username', 'start_offset', 'end_time', 'video_duration']].get_values()
            # key = (course_id, encoded_module_id)
            pipeline_video_id = '{0}|{1}'.format(course_id, encoded_module_id)
            usage_map = {}

            video_duration = 0
            for viewing in values:
                username, start_offset, end_offset, duration = viewing

                # Find the maximum actual video duration, but indicate that
                # it's unknown if any viewing was of a video with unknown duration.
                duration = float(duration)
                if video_duration == VIDEO_UNKNOWN_DURATION:
                    pass
                elif duration == VIDEO_UNKNOWN_DURATION:
                    video_duration = VIDEO_UNKNOWN_DURATION
                elif duration > video_duration:
                    video_duration = duration

                first_segment = self.snap_to_last_segment_boundary(float(start_offset))
                last_segment = self.snap_to_last_segment_boundary(float(end_offset))
                for segment in xrange(first_segment, last_segment + 1):
                    stats = usage_map.setdefault(segment, {})
                    users = stats.setdefault('users', set())
                    users.add(username)
                    stats['views'] = stats.get('views', 0) + 1

            # If we don't know the duration of the video, just use the final segment that was
            # actually viewed to determine users_at_end.
            if video_duration == VIDEO_UNKNOWN_DURATION:
                final_segment = self.get_final_segment(usage_map)
                video_duration = ((final_segment + 1) * VIDEO_VIEWING_SECONDS_PER_SEGMENT) - 1
            else:
                final_segment = self.snap_to_last_segment_boundary(float(video_duration))

            # Output stats.
            users_at_start = len(usage_map.get(0, {}).get('users', []))
            users_at_end = len(usage_map.get(self.complete_end_segment(video_duration), {}).get('users', []))
            for segment in sorted(usage_map.keys()):
                stats = usage_map[segment]
                # yield VideoSegmentDetailRecord(
                #     pipeline_video_id=pipeline_video_id,
                #     course_id=course_id,
                #     encoded_module_id=encoded_module_id,
                #     duration=int(video_duration),
                #     segment_length=VIDEO_VIEWING_SECONDS_PER_SEGMENT,
                #     users_at_start=users_at_start,
                #     users_at_end=users_at_end,
                #     segment=segment,
                #     num_users=len(stats.get('users', [])),
                #     num_views=stats.get('views', 0)
                # ).to_string_tuple()
                yield (
                    pipeline_video_id,
                    course_id,
                    encoded_module_id,
                    int(video_duration),
                    VIDEO_VIEWING_SECONDS_PER_SEGMENT,
                    users_at_start,
                    users_at_end,
                    segment,
                    len(stats.get('users', [])),
                    stats.get('views', 0)
                )
                if segment == final_segment:
                    break

    @property
    def record_filter(self):
        return None

    def requires_local(self):
        return VideoUsageTask(
            interval=self.overwrite_interval,
            source=self.source,
            pattern=self.pattern
        )

    def requires(self):
        yield super(VideoTimelineDataTask, self).requires()['credentials']

        requires_local = self.requires_local()
        if isinstance(requires_local, luigi.Task):
            yield requires_local


class InsertToMysqlVideoTimelineTask(VideoTableDownstreamMixin, MysqlInsertTask):
    """Insert information about video timelines from a Hive table into MySQL."""

    overwrite = luigi.BooleanParameter(
        default=True,
        description='Overwrite the table when writing to it by default. Allow users to override this behavior if they '
                    'want.',
        significant=False
    )
    allow_empty_insert = luigi.BooleanParameter(
        default=True,
        description='Allow the video table to be empty (e.g. if no video activity has occurred)',
        config_path={'section': 'videos', 'name': 'allow_empty_insert'},
        significant=False,
    )

    @property
    def table(self):  # pragma: no cover
        return 'video_timeline'

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def columns(self):  # pragma: no cover
        return VideoTimelineRecord.get_sql_schema()

    @property
    def insert_query(self):
        return """
            SELECT
                pipeline_video_id,
                segment,
                num_users,
                num_views
            FROM video_usage
        """

    def rows(self):
        log.info('query_sql = [{}]'.format(self.insert_query))
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        for row in query_result:
            yield row
        #
        # pipeline_video_id = StringField(length=255, nullable=False)
        # segment = IntegerField()
        # num_users = IntegerField()
        # num_views = IntegerField()

        # yield ('test-pipeline_video_id', 1, 2, 3)

    @property
    def indexes(self):  # pragma: no cover
        return [
            ('pipeline_video_id',),
        ]

    def requires(self):
        yield super(InsertToMysqlVideoTimelineTask, self).requires()['credentials']
        # the process that generates the source table used by this query
        yield (
            VideoTimelineDataTask(
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                overwrite_n_days=self.overwrite_n_days,
            )
        )


class InsertToMysqlVideoTask(VideoTableDownstreamMixin, MysqlInsertTask):
    """Insert summary information into the video table in MySQL."""

    overwrite = luigi.BooleanParameter(
        default=True,
        description='Overwrite the table when writing to it by default. Allow users to override this behavior if they '
                    'want.',
        significant=False
    )
    allow_empty_insert = luigi.BooleanParameter(
        default=False,
        description='Allow the video table to be empty (e.g. if no video activity has occurred)',
        config_path={'section': 'videos', 'name': 'allow_empty_insert'},
        significant=False,
    )

    @property
    def table(self):  # pragma: no cover
        return 'video'

    @property
    def insert_source_task(self):  # pragma: no cover
        return None

    @property
    def columns(self):  # pragma: no cover
        return VideoSegmentSummaryRecord.get_sql_schema()

    @property
    def insert_query(self):  # pragma: no cover
        """The fields used in the source table."""
        return """
                SELECT
                    pipeline_video_id,
                    course_id,
                    encoded_module_id,
                    duration,
                    segment_length,
                    users_at_start,
                    users_at_end,
                    sum(num_views) * segment_length
                FROM video_usage
                GROUP BY
                    pipeline_video_id,
                    course_id,
                    encoded_module_id,
                    duration,
                    segment_length,
                    users_at_start,
                    users_at_end
            """

    def rows(self):
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.insert_query)
        log.info('query_sql = [{}]'.format(self.insert_query))
        for row in query_result:
            yield row

    @property
    def indexes(self):  # pragma: no cover
        return [
            ('course_id', 'encoded_module_id'),
        ]

    def requires(self):
        yield super(InsertToMysqlVideoTask, self).requires()['credentials']
        yield (
            VideoTimelineDataTask(
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                overwrite_n_days=self.overwrite_n_days,
            )
        )


@workflow_entry_point
class HylInsertToMysqlAllVideoTask(VideoTableDownstreamMixin, luigi.WrapperTask):
    """Insert all video data into MySQL."""

    def requires(self):
        kwargs = {
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite_n_days': self.overwrite_n_days,
        }
        yield (
            InsertToMysqlVideoTimelineTask(**kwargs),
            InsertToMysqlVideoTask(**kwargs),
        )
