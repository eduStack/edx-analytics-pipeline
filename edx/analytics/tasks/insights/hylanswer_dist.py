"""
Luigi tasks for extracting problem answer distribution statistics from
tracking log files.
"""
import csv
import gzip
import hashlib
import json
import logging
import math
from operator import itemgetter

import html5lib
import luigi.s3
import pandas as pd
from luigi.configuration import get_config

import edx.analytics.tasks.util.eventlog as eventlog
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.data import LoadEventFromMongoTask
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTaskMixin, MysqlTableTask
from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


def get_text_from_html(markup):
    """
    Convert html markup to plain text.

    Includes stripping excess whitespace, and assuring whitespace
    exists between elements (e.g. table elements).
    """
    try:
        root = html5lib.parse(markup)
        text_list = []
        for val in get_text_from_element(root):
            text_list.extend(val.split())
        text = u' '.join(text_list)
    except Exception as exception:  # pylint: disable=broad-except
        # TODO: find out what exceptions might actually occur here, if any.
        # This may be unnecessarily paranoid, given html5lib's fallback behavior.
        log.error("Unparseable answer value markup: '%s' return exception %s", markup, exception)
        text = markup.strip()

    return text


def get_text_from_element(node):
    """Traverse ElementTree node recursively to return text values."""
    tag = node.tag
    if not isinstance(tag, basestring) and tag is not None:
        return
    if node.text:
        yield node.text
    for child in node:
        for text in get_text_from_element(child):
            yield text
        if child.tail:
            yield child.tail


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


class ProblemCheckEvent(AnswerDistributionDownstreamMixin, LoadEventFromMongoTask):
    interval = None

    def init_env(self):
        pass

    def is_hidden_answer(self, answer_id):
        """Check Id to identify hidden kinds of values."""
        # some problems have additional answers that have '_dynamath' appended
        # to the regular answer_id.  In this case, the contents seem to contain
        # something like:
        #
        # <math xmlns="http://www.w3.org/1998/Math/MathML">
        #   <mstyle displaystyle="true">
        #     <mo></mo>
        #   </mstyle>
        # </math>
        if answer_id.endswith('_dynamath'):
            return True

        # Others seem to end with _comment, and I don't know yet what these
        # look like.
        if answer_id.endswith('_comment'):
            return True

        return False

    def _generate_answers(self, event_string, attempt_category):
        """
        Generates a list of answers given a problem_check event.

        Args:
            event_string:  a json-encoded string version of an event's data.
            attempt_category: a string that is 'first' for a user's first response to a question, 'last' otherwise

        Returns:
            list of answer data tuples.

        See docstring for reducer() for more details.
        """
        event = json.loads(event_string)

        # Get context information:
        course_id = eventlog.get_course_id(event)
        timestamp = event.get('timestamp')
        problem_id = event.get('problem_id')
        grade = event.get('grade')
        max_grade = event.get('max_grade')
        problem_display_name = event.get('context').get('module', {}).get('display_name', None)
        result = []

        def append_submission(answer_id, submission):
            """Convert submission to result to be returned."""
            # First augment submission with problem-level information
            # not found in the submission:
            submission['problem_id'] = problem_id
            submission['problem_display_name'] = problem_display_name
            submission['attempt_category'] = attempt_category
            submission['grade'] = grade
            submission['max_grade'] = max_grade

            # Add the timestamp so that all responses can be sorted in order.
            # We want to use the "latest" values for some fields.
            # output_key = (course_id, answer_id)
            # output_value = (timestamp, json.dumps(submission))
            result.append((course_id, answer_id, timestamp, json.dumps(submission)))

        answers = event.get('answers')
        correct_map = event.get('correct_map', {})
        if 'submission' in event:
            submissions = event.get('submission')
            for answer_id in submissions:
                if not self.is_hidden_answer(answer_id):
                    submission = submissions.get(answer_id)
                    # But submission doesn't contain moniker value for answer.
                    # So we check the raw answers, and see if its value is
                    # different.  If so, we assume it's a moniker.
                    answer_value = answers[answer_id]
                    if answer_value != submission.get('answer'):
                        submission['answer_value_id'] = answer_value

                    submission['answer_correct_map'] = correct_map.get(answer_id)
                    append_submission(answer_id, submission)

        else:
            # Otherwise, it's an older event with no 'submission'
            # information, so parse it as well as possible.
            for answer_id in answers:
                if not self.is_hidden_answer(answer_id):
                    answer_value = answers[answer_id]

                    # Argh. It seems that sometimes we're encountering
                    # bogus answer_id values.  In particular, one that
                    # is including the possible choice values, instead
                    # of any actual values selected by the student.
                    # For now, let's just dump an error and skip it,
                    # so that it becomes the equivalent of a hidden
                    # answer.

                    # TODO: Eventually treat it explicitly as a hidden
                    # answer.
                    if answer_id not in correct_map:
                        log.error("Unexpected answer_id %s not in correct_map: %s", answer_id, event)
                        continue
                    correctness = correct_map[answer_id].get('correctness') == 'correct'

                    variant = event.get('state', {}).get('seed')

                    # We do not know the values for 'input_type',
                    # 'response_type', or 'question'.  We also don't know if
                    # answer_value should be identified as 'answer_value_id' or
                    # 'answer', so we choose to use 'answer_value_id' here and
                    # never define 'answer'.  This allows disambiguation from
                    # events with a submission field, which will always have
                    # an 'answer' and only sometimes have an 'answer_value_id'.
                    submission = {
                        'answer_value_id': answer_value,
                        'correct': correctness,
                        'variant': variant,
                        'answer_correct_map': correct_map.get(answer_id),
                    }
                    append_submission(answer_id, submission)

        return result

    def event_filter(self):
        filter = {
            '$and': [
                {'event_type': 'problem_check'},
                {'event_source': 'server'}
            ]}
        return filter

    def get_event_row_from_document(self, document):
        event_row = get_problem_check_event(document)
        if not event_row:
            return

        (course_id, problem_id, username), (timestamp, problem_check_info) = event_row
        event_row = (course_id, problem_id, username, timestamp, problem_check_info)
        return event_row

    def processing(self, raw_events):
        columns = ['course_id', 'problem_id', 'username', 'timestamp', 'problem_check_info']
        # log.info('raw_events = {}'.format(raw_events))
        df = pd.DataFrame(data=raw_events, columns=columns)
        result = []
        for (course_id, problem_id, username), group in df.groupby(['course_id', 'problem_id', 'username']):
            values = group[['timestamp', 'problem_check_info']].get_values()
            values = sorted(values, key=lambda x: x[0])

            # Get the first entry.
            _timestamp, first_event = values[0]

            for answer in self._generate_answers(first_event, 'first'):
                result.append(answer)

            # Get the last entry.
            _timestamp, most_recent_event = values[-1]

            for answer in self._generate_answers(most_recent_event, 'last'):
                result.append(answer)
        # log.info('result = {}'.format(result))
        return result


class AnswerDistributionPerCourse(AnswerDistributionDownstreamMixin, luigi.Task):
    """Calculates answer distribution on a problem in a course, given per-user answers by date."""

    completed = False

    def complete(self):
        return self.completed

    def reducer(self, key, values):
        """
        Calculate a JSON dict for each unique answer to a problem in a course.

        Args:
            key:  (course_id, answer_id)
            values:  iterator of (timestamp, answer_data)

        Yields:
            list of answer data tuples, where a tuple consists of:

                course_id, answer_json

            where answer_json is a JSON string corresponding to a
            particular response value to a particular "answer" within
            a problem.  The JSON includes metadata about the particular
            answer, the value of the answer, and the count of how many
            users for whom it was an answer.

        """
        course_id, answer_id = key

        values = sorted(values, key=lambda x: x[0])
        if not values:
            return

        # Get the last entry.  We will use its values to provide
        # metadata about the particular answer.
        _timestamp, most_recent_answer_string = values[-1]
        most_recent_answer = json.loads(most_recent_answer_string)

        self.add_metadata_to_answer(answer_id, most_recent_answer)

        # Determine if any answers should be included based on
        # information in the most recent answer.
        if not self.should_include_answer(most_recent_answer):
            return

        # Now construct answer distribution for this input.
        problem_id = most_recent_answer.get('problem_id')
        problem_display_name = most_recent_answer.get('problem_display_name')
        answer_uses_value_id = ('answer_value_id' in most_recent_answer)
        answer_dist = {}
        for _timestamp, value_string in reversed(values):
            answer = json.loads(value_string)
            self.add_metadata_to_answer(answer_id, answer)
            answer_grouping_key = self.get_answer_grouping_key(answer)

            # TODO: add check here to see if the number of distinct
            # variants for the problem is high enough to trigger
            # abandoning the output of the distribution.

            # If this is the first entry we find that has this value,
            # then save out the relevant metadata about this value.
            # We only want this from the most recent answer that has
            # this value.
            if answer_grouping_key not in answer_dist:
                if answer_uses_value_id:
                    # The most recent overall answer indicates that
                    # the code should be returned as such.  If this
                    # particular answer did not have 'submission'
                    # information, it may not have an answer_value, so
                    # we flag it. The problem type may have changed as
                    # well, so previous answers to this problem may not
                    # actually have an answer_value_id even though the
                    # most recent one does.
                    value_id = answer.get('answer_value_id', '')
                    answer_value = answer.get('answer', UNKNOWN_ANSWER_VALUE)
                else:
                    # There should be no value_id returned.  If the
                    # current answer did not have 'submission'
                    # information, then move the value from the
                    # 'answer_value_id' to the 'answer' field.
                    value_id = ""
                    answer_value = answer.get('answer', answer.get('answer_value_id'))

                # These values may be lists, so convert to output format.
                # And if we have a value_id, the corresponding answer_value
                # may contain HTML markup, that should be stripped.
                # But don't strip markup otherwise, as it may be part of
                # the answer.
                value_id = self.stringify(value_id)
                answer_value_contains_html = (value_id is not None and value_id != '')
                answer_value = self.stringify(answer_value, contains_html=answer_value_contains_html)

                # Even if the most recent answer does not have a variant,
                # the question could have been randomized earlier and then
                # switched, so the variant must be checked for all answers.
                question = answer.get('question', '')
                variant = answer.get('variant') or ''

                # Key values here should match those used in get_column_order().
                answer_dist[answer_grouping_key] = {
                    'ModuleID': problem_id,
                    'PartID': answer_id,
                    'ValueID': value_id or '',
                    'AnswerValue': answer_value or '',
                    'Variant': variant,
                    'Problem Display Name': problem_display_name or '',
                    'Question': question,
                    'Correct Answer': '1' if answer.get('correct') else '0',
                    'First Response Count': 0,
                    'Last Response Count': 0,
                }

            # For most cases, just increment a counter:
            attempt_category = answer.get('attempt_category')
            if attempt_category == 'first':
                answer_dist[answer_grouping_key]['First Response Count'] += 1
            elif attempt_category == 'last':
                answer_dist[answer_grouping_key]['Last Response Count'] += 1
            else:
                raise RuntimeError('Unknown attempt category: {0}'.format(attempt_category))

        # Finally dispatch the answers, providing the course_id as a
        # key so that the answers belonging to a course will be
        # gathered downstream into a report.
        for answer_entry in answer_dist.values():
            # Transform the entry into a form suitable for output.
            yield course_id, json.dumps(answer_entry)

    def add_metadata_to_answer(self, answer_id, answer):
        """
        Add externally-provided metadata for answers that lack it.

        See docstring for load_answer_metadata() for list of fields.

        Adds these fields to the answer if the answer lacks a
        non-empty value.  Uses the answer_value_id_map to provide a
        corresponding 'answer' when only an 'answer_value_id' is
        available.  These are done for answers that are derived from
        problem_check events that lack these fields, because they
        occurred before the information was added to events.

        """
        # The 'answer_metadata_dict' should only exist if load_answer_metadata() is called.
        answer_metadata = getattr(self, 'answer_metadata_dict', {}).get(answer_id)
        if answer_metadata is not None:
            for key, value in answer_metadata.iteritems():
                # Should only add values that are not already present
                # (and non-null).  Also skips over values that are not
                # strings (such as the answer_value_id_map), as this is
                # handled separately below.
                if not answer.get(key) and isinstance(value, basestring):
                    answer[key] = value

            if 'answer' not in answer:
                response_type = answer.get('response_type')
                if response_type in ['choiceresponse', 'multiplechoiceresponse']:
                    # We leave what we have in 'answer_value_id', and look
                    # up the 'answer' to use from the
                    # answer_metadata_dict, based on the value(s) in
                    # 'answer_value_id'.
                    if 'answer_value_id_map' in answer_metadata:
                        answer_value_id = answer['answer_value_id']
                        answer_value_id_map = answer_metadata['answer_value_id_map']

                        def get_answer_value(code):
                            return answer_value_id_map.get(code, UNMAPPED_ANSWER_VALUE)

                        if isinstance(answer_value_id, basestring):
                            answer['answer'] = get_answer_value(answer_value_id)
                        elif isinstance(answer_value_id, list):
                            answer['answer'] = [get_answer_value(code) for code in answer_value_id]
                else:
                    # The 'answer_value_id' is really the 'answer', so move it.
                    answer['answer'] = answer['answer_value_id']
                    del answer['answer_value_id']

    def should_include_answer(self, answer):
        """Determine if a problem "part" should be included in the distribution."""
        response_type = answer.get('response_type')

        # For problems which only have old responses, we don't
        # have information about whether to include their answers.
        if response_type is None:
            return False

        # read out valid types from client.cfg file.  The 3rd argument below sets a default in case the
        # config file is somehow misread.  But to change the list, please update the client.cfg
        valid_type_str = get_config().get(
            'answer-distribution',
            'valid_response_types',
            'choiceresponse,optionresponse,multiplechoiceresponse,numericalresponse,stringresponse,formularesponse'
        )

        valid_types = set(valid_type_str.split(","))
        if response_type in valid_types:
            return True

        return False

    def get_answer_grouping_key(self, answer):
        """Return value to use for uniquely identify an answer value in the distribution."""
        # For variants, we want to treat missing variants with the
        # same value as used for events that lack 'submission'
        # information, so that they will be grouped together.  That
        # value is a seed value of '1'.  We want to map both missing
        # values and zero-length values to this default value.
        variant = answer.get('variant', '')
        if variant == '':
            variant = '1'
        # Events that lack 'submission' information will have a value
        # for 'answer_value_id' and none for 'answer'.  Events with
        # 'submission' information will have the reverse situation
        # most of the time, but both values filled in for multiple
        # choice.  In the latter case, we need to use the
        # answer_value_id for comparison.
        if 'answer_value_id' in answer:
            answer_value = answer.get('answer_value_id')
        else:
            answer_value = answer.get('answer')

        # answer_value may be a list of multiple values, so we need to
        # convert it to a string that can be used as an index (i.e. to
        # increment a previous occurrence).
        return u'{value}_{variant}'.format(value=self.stringify(answer_value), variant=variant)

    @staticmethod
    def stringify(answer_value, contains_html=False):
        """
        Convert answer value to a canonical string representation.

        If answer_value is a list, then returns list values
        surrounded by square brackets and delimited by pipes
        (e.g. "[choice_1|choice_3|choice_4]").

        If answer_value is a string, just returns as-is.

        If contains_html is True, the answer_string is parsed as XML,
        and the text value of the answer_value is returned.

        """

        # If it's a list, convert to a string.  Note that it's not
        # enough to call str() or unicode(), as this will appear as
        # "[u'choice_5']".
        def normalize(value):
            """Pull out HTML tags if requested."""
            return get_text_from_html(value) if contains_html else value.strip()

        if isinstance(answer_value, basestring):
            return normalize(answer_value)
        elif isinstance(answer_value, list):
            list_val = u'|'.join(normalize(value) for value in answer_value)
            return u'[{list_val}]'.format(list_val=list_val)
        else:
            # unexpected type:
            log.error("Unexpected type for an answer_value: %s", answer_value)
            return unicode(answer_value)

    def requires_local(self):
        return ProblemCheckEvent(
            name=self.name,
            src=self.src,
            dest=self.dest,
            include=self.include,
            manifest=self.manifest,
        )

    def output(self):
        require = self.requires_local()
        if require:
            events = require.output()
            columns = ['course_id', 'answer_id', 'timestamp', 'answer_data']

            df = pd.DataFrame(data=events, columns=columns)
            for (course_id, answer_id), group in df.groupby(['course_id', 'answer_id']):
                values = group[['timestamp', 'answer_data']].get_values()
                key = (course_id, answer_id)
                yield self.reducer(key, values)

        # output_name = u'answer_distribution_per_course_{name}/'.format(name=self.name)
        # return get_target_from_url(url_path_join(self.dest, output_name))

    def requires(self):
        yield self.requires_local()

    #
    #     if self.answer_metadata:
    #         results.update({'answer_metadata': ExternalURL(self.answer_metadata)})
    #

    def run(self):
        if not self.completed:
            self.completed = True
        # Define answer_metadata on the object if specified.
        # if 'answer_metadata' in self.input():
        #     with self.input()['answer_metadata'].open('r') as answer_metadata_file:
        #         self.load_answer_metadata(answer_metadata_file)

        # super(AnswerDistributionPerCourse, self).run()


class AnswerDistributionToMySQLTaskWorkflow(AnswerDistributionDownstreamMixin, MysqlTableTask):
    """
    Define answer_distribution table.
    """
    # Override the parameter that normally defaults to false. This ensures that the table will always be overwritten.
    overwrite = luigi.BooleanParameter(default=True, significant=False)

    def rows(self):
        """
        Re-formats the output of AnswerDistributionPerCourse to something insert_rows can understand
        """
        require = self.requires_local()
        if require:
            for rows in require.output():
                for row in rows:
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
        for req in super(AnswerDistributionToMySQLTaskWorkflow, self).requires():
            yield req
        yield self.requires_local()


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
