"""
Luigi tasks for extracting problem answer distribution statistics from
tracking log files.
"""
import csv
import hashlib
import json
import logging
import math
from operator import itemgetter

import html5lib
import luigi.s3
from luigi.configuration import get_config

import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask, MysqlInsertTaskMixin
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)

################################
# Task Map-Reduce definitions
################################

UNKNOWN_ANSWER_VALUE = ''
UNMAPPED_ANSWER_VALUE = ''


##################################
# Task requires/output definitions
##################################

class BaseAnswerDistributionDownstreamMixin(object):
    """
    Base class for answer distribution calculations.

    """
    name = luigi.Parameter(
        description='A unique identifier to distinguish one run from another.  It is used in '
                    'the construction of output filenames, so each run will have distinct outputs.',
    )
    src = luigi.Parameter(
        is_list=True,
        description='A list of URLs to the root location of input tracking log files.',
    )
    dest = luigi.Parameter(
        description='A URL to the root location to write output file(s).',
    )
    include = luigi.Parameter(
        is_list=True,
        default=('*',),
        description='A list of patterns to be used to match input files, relative to `src` URL. '
                    'The default value is [\'*\'].',
    )
    manifest = luigi.Parameter(
        default=None,
        description='A URL to a file location that can store the complete set of input files. '
                    'A manifest file is required by Hadoop if it hits the OS limit on the length '
                    'of the command to run when launching the job using the Hadoop CLI. '
                    'This file will be written to if it doesn\'t exist, and read from if it already does.',
    )


class AnswerDistributionDownstreamMixin(BaseAnswerDistributionDownstreamMixin):
    """
    Parameters needed for calculating answer distribution.
    """
    answer_metadata = luigi.Parameter(
        default=None,
        description="optional file to provide information about particular answers. "
                    "Includes problem_display_name, input_type, response_type, and question.",
    )

    base_input_format = luigi.Parameter(
        default=None,
        description="The input format to use on the first map reduce job in the chain. "
                    "This job takes in the most input and may need a custom input format.",
    )


class AnswerDistributionPerCourse(AnswerDistributionDownstreamMixin, luigi.Task):
    """Calculates answer distribution on a problem in a course, given per-user answers by date."""

    completed = False

    def complete(self):
        return self.completed

    # def requires(self):
    #     results = {
    #         'events': ProblemCheckEvent(
    #             input_format=self.base_input_format,
    #             name=self.name,
    #             src=self.src,
    #             dest=self.dest,
    #             include=self.include,
    #             manifest=self.manifest,
    #         ),
    #     }
    #
    #     if self.answer_metadata:
    #         results.update({'answer_metadata': ExternalURL(self.answer_metadata)})
    #
    #     return results

    def output(self):
        answer_dist = {
            'ModuleID': 'test',
            'PartID': 'test',
            'ValueID': '1',
            'AnswerValue': 0 or '',
            'Variant': '',
            'Problem Display Name': 'test',
            'Question': 'test',
            'Correct Answer': '1',
            'First Response Count': 0,
            'Last Response Count': 0,
        }
        yield ('test_id', json.dumps(answer_dist))
        # output_name = u'answer_distribution_per_course_{name}/'.format(name=self.name)
        # return get_target_from_url(url_path_join(self.dest, output_name))

    def run(self):
        if not self.completed:
            self.completed = True
        # Define answer_metadata on the object if specified.
        # if 'answer_metadata' in self.input():
        #     with self.input()['answer_metadata'].open('r') as answer_metadata_file:
        #         self.load_answer_metadata(answer_metadata_file)

        # super(AnswerDistributionPerCourse, self).run()


class AnswerDistributionToMySQLTaskWorkflow(AnswerDistributionDownstreamMixin, MysqlInsertTask):
    """
    Define answer_distribution table.
    """
    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BooleanParameter(default=True, significant=False)

    @property
    def insert_source_task(self):
        return None

    def rows(self):
        """
        Re-formats the output of AnswerDistributionPerCourse to something insert_rows can understand
        """
        require = self.requires_local()
        if require:
            for row in require.output():
                course_id, content = row
                row_dict = json.loads(content)
                output_list = [
                    course_id,
                    row_dict['ModuleID'],
                    row_dict['PartID'],
                    row_dict['Correct Answer'],
                    row_dict['First Response Count'],
                    row_dict['Last Response Count'],
                    row_dict['ValueID'] if row_dict['ValueID'] else None,
                    row_dict['AnswerValue'] if row_dict['AnswerValue'] else None,
                    try_str_to_float(row_dict['AnswerValue']),
                    row_dict['Variant'] if row_dict['Variant'] else None,
                    row_dict['Problem Display Name'] if row_dict['Problem Display Name'] else None,
                    row_dict['Question'] if row_dict['Question'] else None,
                ]
                yield output_list

    @property
    def table(self):
        return "answer_distribution"

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('module_id', 'VARCHAR(255) NOT NULL'),
            ('part_id', 'VARCHAR(255) NOT NULL'),
            ('correct', 'TINYINT(1) NOT NULL'),
            ('first_response_count', 'INT(11) NOT NULL'),
            ('last_response_count', 'INT(11) NOT NULL'),
            ('value_id', 'VARCHAR(255)'),
            ('answer_value_text', 'LONGTEXT'),
            ('answer_value_numeric', 'DOUBLE'),
            ('variant', 'INT(11)'),
            ('problem_display_name', 'LONGTEXT'),
            ('question_text', 'LONGTEXT'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id',),
            ('module_id',),
            ('part_id',),
            ('course_id', 'module_id'),
        ]

    def requires_local(self):
        return AnswerDistributionPerCourse(
            name=self.name,
            src=self.src,
            dest=self.dest,
            include=self.include,
            manifest=self.manifest,
        )

    def requires(self):
        yield super(AnswerDistributionToMySQLTaskWorkflow, self).requires()['credentials']

        requires_local = self.requires_local()
        if isinstance(requires_local, luigi.Task):
            yield requires_local


@workflow_entry_point
class HylAnswerDistributionWorkflow(
    AnswerDistributionDownstreamMixin,
    MysqlInsertTaskMixin,
    luigi.WrapperTask):
    """Calculate answer distribution and output to files and to database."""

    # Add additional args for MultiOutputMapReduceJobTask.
    output_root = luigi.Parameter(
        description='Directory to store the output in.',
    )
    marker = luigi.Parameter(
        description='A URL location where a marker file should be written. '
                    'Note: the task will not run if the marker file already exists.',
    )

    def requires(self):
        kwargs = {
            'name': self.name,
            'src': self.src,
            'dest': self.dest,
            'include': self.include,
            'manifest': self.manifest,
            'answer_metadata': self.answer_metadata,
            'base_input_format': self.base_input_format,
        }

        # Add additional args for MultiOutputMapReduceJobTask.
        kwargs1 = {
            'output_root': self.output_root,
            'marker': self.marker,
        }
        kwargs1.update(kwargs)

        # Add additional args for MysqlInsertTaskMixin.
        kwargs2 = {
            'database': self.database,
            'credentials': self.credentials,
            'insert_chunk_size': self.insert_chunk_size,
        }
        kwargs2.update(kwargs)

        yield (
            # AnswerDistributionOneFilePerCourseTask(**kwargs1),
            AnswerDistributionToMySQLTaskWorkflow(**kwargs2),
        )


################################
# Helper methods
################################
def try_str_to_float(value_str):
    try:
        float_val = float(value_str)
        # infinity values and NaN actually break mysql-connector (because they look like a string), so make those None
        if math.isinf(float_val) or math.isnan(float_val):
            return None
        return float_val
    except (ValueError, TypeError):
        return None


def get_problem_check_event(line_or_event):
    """
    Generates output values for explicit problem_check events.

    Args:

        line_or_event: pre-parsed event dict, or text line from a tracking event log

    Returns:

        (problem_id, username), (timestamp, problem_check_info)

        where timestamp is in ISO format, with resolution to the millisecond
        and problem_check_info is a JSON-serialized dict containing
        the contents of the problem_check event's 'event' field,
        augmented with entries for 'timestamp', 'username', and
        'context' from the event.

        or None if there is no valid problem_check event on the line.

    Example:
            (i4x://edX/DemoX/Demo_Course/problem/PS1_P1, dummy_username), (2013-09-10T00:01:05.123456, blah)

    """
    # Ensure the given event dict is a problem_check event
    if isinstance(line_or_event, dict):
        event = line_or_event
        if event.get('event_type') != 'problem_check':
            return None

    # Parse the line into an event dict, if not provided.
    else:
        event = eventlog.parse_json_server_event(line_or_event, 'problem_check')
        if event is None:
            return None

    # Get the "problem data".  This is the event data, the context, and anything else that would
    # be useful further downstream.  (We could just pass the entire event dict?)

    # Get the user from the username, not from the user_id in the
    # context.  While we are currently requiring context (as described
    # above), we might not in future.  Older events will not have
    # context information, so we can't rely on user_id from there.
    # And we don't expect problem_check events to occur without a
    # username, and don't expect them to occur with the wrong user
    # (i.e. one user acting on behalf of another, as in an instructor
    # acting on behalf of a student).
    augmented_data_fields = ['context', 'username', 'timestamp']
    problem_data = eventlog.get_augmented_event_data(event, augmented_data_fields)
    if problem_data is None:
        return None

    # Get the course_id from context.  We won't work with older events
    # that do not have context information, since they do not directly
    # provide course_id information.  (The problem_id/answer_id values
    # contain the org and course name, but not the run.)  Course_id
    # information could be found from other events, but it would
    # require expanding the events being selected.
    course_id = eventlog.get_course_id(event)
    if course_id is None:
        log.error("encountered explicit problem_check event with missing course_id: %s", event)
        return None

    if not opaque_key_util.is_valid_course_id(course_id):
        log.error("encountered explicit problem_check event with bogus course_id: %s", event)
        return None

    # Get the problem_id from the event data.
    problem_id = problem_data.get('problem_id')
    if problem_id is None:
        log.error("encountered explicit problem_check event with bogus problem_id: %s", event)
        return None

    event = event.get('event', {})
    answers = event.get('answers', {})
    if len(answers) == 0:
        return None

    try:
        _check_answer_ids(answers)
        _check_answer_ids(event.get('submission', {}))
    except (TypeError, ValueError):
        log.error("encountered explicit problem_check event with invalid answers: %s", event)
        return None

    problem_data_json = json.dumps(problem_data)
    key = (course_id, problem_id, problem_data.get('username'))
    value = (problem_data.get('timestamp'), problem_data_json)

    return key, value


def _check_answer_ids(answer_dict):
    if not isinstance(answer_dict, dict):
        raise TypeError('Expected dictionaries for answers')

    for answer_id in answer_dict:
        if '\n' in answer_id or '\t' in answer_id:
            raise ValueError('Malformed answer_id')

        try:
            answer_id.encode('ascii')
        except UnicodeEncodeError:
            raise ValueError('Non-ascii answer_id')
