"""Tasks for aggregating statistics about video viewing."""
import datetime
import json
import logging
import math
import re
import textwrap
import urllib
from collections import namedtuple

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


class VideoTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the VideoUsageTask and its required tasks."""
    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'videos', 'name': 'overwrite_n_days'},
        significant=False,
        default=3,
    )


class VideoUsageTask(VideoTableDownstreamMixin, luigi.Task):
    completed = False

    def complete(self):
        return self.completed

    def output(self):
        rows = [
            ('course-v1:BISTU_JSZX+0BS11002+2016_2017_T2|8f2f3f3078954b009d733cb042281e9e',
             'course-v1:BISTU_JSZX+0BS11002+2016_2017_T2',
             '8f2f3f3078954b009d733cb042281e9e', 289, 5, 14, 14, 0, 14, 24),
            ('course-v1:BISTU_JSZX+0BS11002+2016_2017_T2|8f2f3f3078954b009d733cb042281e9e',
             'course-v1:BISTU_JSZX+0BS11002+2016_2017_T2',
             '8f2f3f3078954b009d733cb042281e9e', 289, 5, 14, 14, 0, 14, 24),
            ('course-v1:BISTU_JSZX+0BS11002+2016_2017_T2|8f2f3f3078954b009d733cb042281e9e',
             'course-v1:BISTU_JSZX+0BS11002+2016_2017_T2',
             '8f2f3f3078954b009d733cb042281e9e', 289, 5, 14, 14, 0, 14, 24),
            ('course-v1:BISTU_JSZX+0BS11002+2016_2017_T2|8f2f3f3078954b009d733cb042281e9e',
             'course-v1:BISTU_JSZX+0BS11002+2016_2017_T2',
             '8f2f3f3078954b009d733cb042281e9e', 289, 5, 14, 14, 0, 14, 24),
            ('course-v1:BISTU_JSZX+0BS11002+2016_2017_T2|8f2f3f3078954b009d733cb042281e9e',
             'course-v1:BISTU_JSZX+0BS11002+2016_2017_T2',
             '8f2f3f3078954b009d733cb042281e9e', 289, 5, 14, 14, 0, 14, 24),
        ]
        for row in rows:
            yield row

    def run(self):
        log.info('test VideoUsageTask')
        if self.completed is False:
            self.completed = True


class VideoTimelineDataTask(VideoTableDownstreamMixin, IncrementalMysqlInsertTask):

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

    def rows(self):
        require = self.requires_local()
        if require:
            for row in require.output():
                yield row

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
            # InsertToMysqlVideoTask(**kwargs),
        )
