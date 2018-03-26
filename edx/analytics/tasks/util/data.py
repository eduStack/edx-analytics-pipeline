import luigi
import logging
import traceback

log = logging.getLogger(__name__)


class UniversalDataTask(luigi.Task):
    result = []
    completed = False

    def __init__(self, *args, **kwargs):
        super(UniversalDataTask, self).__init__(*args, **kwargs)

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
