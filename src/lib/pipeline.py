from pyspark.ml import Pipeline

import sys
sys.path.insert(0, "./lib")
from ETL import GenreSplitter 

class Pipe:

    def __init__(self, df, input_col):
        self.df = df
        self.input_col = input_col

    def create_pipeline(self, input_col):
        genre_splitter = GenreSplitter(input_col)
        return Pipeline(stages=[genre_splitter])

    def fitPipeline(self, pipeline, df):
        return pipeline.fit(df)

    def transformPipeline(self, model, df):
        return model.transform(df)


    def execute(self):
        pipeline = self.create_pipeline(self.input_col)
        model = self.fitPipeline(pipeline, self.df)
        df_transformed = self.transformPipeline(model, self.df)
        return df_transformed

