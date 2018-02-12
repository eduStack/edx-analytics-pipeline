"""Compute metrics related to user enrollments in courses"""

import logging

import luigi.task

from edx.analytics.tasks.util.decorators import workflow_entry_point

log = logging.getLogger(__name__)


@workflow_entry_point
class SetupTest(luigi.Task):

    def run(self):
        log.info('if you see this message, the setup is success!')
