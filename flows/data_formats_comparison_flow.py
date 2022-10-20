import os
from metaflow import FlowSpec, step, S3, profile

URL = 's3://data-etl-dwh-develop/test/data.parquet'


class DataFormatsComparisonFlow(FlowSpec):

    @step
    def start(self):
        import pyarrow.parquet as pq
        with S3() as s3:
            res = s3.get(URL)
            table = pq.read_table(res.path)
            os.rename(res.path, 'taxi.parquet')
            table.to_pandas().to_csv('taxi.csv')
            self.stats = {}
        self.next(self.load_csv, self.load_parquet, self.load_pandas)  # multiple forks

    @step
    def load_csv(self):
        with profile('load_csv', stats_dict=self.stats):
            import csv
            with open('taxi.csv') as csvfile:
                for row in csv.reader(csvfile):  # Not loading in memory
                    pass

        self.next(self.join)

    @step
    def load_parquet(self):
        with profile('load_parquet', stats_dict=self.stats):
            import pyarrow.parquet as pq
            table = pq.read_table('taxi.parquet')
        self.next(self.join)

    @step
    def load_pandas(self):
        with profile('load_pandas', stats_dict=self.stats):
            import pandas as pd
            df = pd.read_parquet('taxi.parquet')
        self.next(self.join)

    @step
    def join(self, inputs):
        self.tags = ['load_csv', 'load_pandas', 'load_parquet']
        self.time = [list(inp.stats.items())[0][1] for inp in inputs]

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DataFormatsComparisonFlow()
