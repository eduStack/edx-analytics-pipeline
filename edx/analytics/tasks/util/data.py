import luigi


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
        try:
            self.completed = self.processing(data)
            # self.output()
        except Exception, e:
            print e

    def processing(self, data):
        self.result = data
        return True

    def load_data(self):
        return []
