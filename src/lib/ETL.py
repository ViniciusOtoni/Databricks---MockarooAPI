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
        
 
        df_pivot = df_exploded.groupBy(*[col for col in df.columns if col != self.input_col]).pivot("genre").agg(F.lit(1))
        
        df_final = df_pivot.fillna(0)
        
        
        genre_columns = [col for col in df_final.columns if col not in df.columns]
        
        for genre in genre_columns:
            new_column_name = f"{self.output_prefix}{genre.lower()}"
            df_final = df_final.withColumnRenamed(genre, new_column_name)
        
        return df_final
