import luigi
import logging
import traceback

from edx.analytics.tasks.common.mysql_load import get_mysql_query_results

from edx.analytics.tasks.insights.database_imports import DatabaseImportMixin
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)


class UniversalDataTask(luigi.Task):
    result = []
    completed = False

    def output(self):
        return self.result

    def complete(self):
        return len(self.result) > 0 or self.completed

    def run(self):
        data = self.load_data()
        log.info('load {} data succ'.format(len(data)))
        try:
            log.info('data processing......')
            self.result = self.processing(data)
            self.completed = True
            log.info('{} data process completed'.format(len(data)))
            # self.output()
        except Exception, e:
            log.error('Porcesing data error:{}'.format(traceback.format_exc()))

    def processing(self, data):
        return data

    def load_data(self):
        return []


class LoadDataFromDatabaseTask(DatabaseImportMixin, UniversalDataTask):

    def load_data(self):
        log.info('query_sql = [{}]'.format(self.query))
        query_result = get_mysql_query_results(credentials=self.credentials, database=self.database,
                                               query=self.query)
        return query_result

    @property
    def query(self):
        """The query builder that controls the structure and fields loaded"""
        raise NotImplementedError

    def requires(self):
        yield ExternalURL(url=self.credentials)
