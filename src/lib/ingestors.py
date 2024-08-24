class IngestaoBronze:
    def __init__(self, spark, catalog_name, table_name, database_name, file_format, partition_fields):

        self.spark = spark
        
        self.catalog = catalog_name
        self.table_name = table_name
        self.database_name = database_name

        self.file_format = file_format
        
        self.partition_fields = partition_fields
        
        
        self.table_fullname = f"{self.catalog}.{self.database_name}.{self.table_name}"

    
    def load(self, path):
        df = (self.spark
              .read
              .format(self.file_format)
              .load(path))
        
        return df

        
    def save(self, df):
        (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(self.table_fullname))
    

    def execute(self, path):
        df = self.load(path)
        self.save(df)


        