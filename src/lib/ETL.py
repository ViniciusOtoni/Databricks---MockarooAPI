from pyspark.sql import DataFrame
from pyspark.ml import Transformer
from pyspark.sql import functions as F

class GenreSplitter(Transformer):
    
    def __init__(self, input_col, output_prefix="genre_"):
        self.input_col = input_col
        self.output_prefix = output_prefix

    def _transform(self, df: DataFrame) -> DataFrame:
    
        df_split = df.withColumn("genre_list", F.split(F.col(self.input_col), "\\|"))
        
        df_exploded = df_split.withColumn("genre", F.explode(F.col("genre_list")))
        
        
        df_pivot = df_exploded.groupBy("movie_title").pivot("genre").agg(F.lit(1))
    
        df_final = df_pivot.fillna(0)
        
        return df_final



