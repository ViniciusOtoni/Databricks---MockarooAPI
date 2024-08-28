import utils

class IngestaoBronze:
    def __init__(self, spark, catalog_name, table_name, database_name, file_format):

        self.spark = spark
        
        self.catalog = catalog_name
        self.table_name = table_name
        self.database_name = database_name

        self.file_format = file_format
        
        
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


class IngestaoSilver:
    def __init__(self, spark, catalog_name, table_name, database_name, file_format):

        self.spark = spark
        
        self.catalog = catalog_name
        self.table_name = table_name
        self.database_name = database_name
        self.file_format = file_format
        
        
        self.table_fullname = f"{self.catalog}.{self.database_name}.{self.table_name}"


    def save(self, df):
        (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(self.table_fullname))
    
    def execute(self, df):
        self.save(df)



class IngestorCubo:

    def __init__(self, spark, catalog, databasename, tablename):
        self.spark = spark
        self.catalog = catalog
        self.databasename = databasename
        self.tablename = tablename
        self.table = f"{catalog}.{databasename}.{tablename}"
        self.set_query()

    def set_query(self):
        self.query = utils.import_query(f"{self.tablename}.sql")

    def load(self, **kwargs):
        formatted_query = self.query.format(**kwargs)
        df = self.spark.sql(formatted_query)
        return df
    
    def save(self, df): 
        if not utils.table_exists(self.spark, self.catalog, self.databasename, self.tablename):
            (df.coalesce(1)
            .write
            .format('delta')
            .mode("overwrite")
            .saveAsTable(self.table))
        else:
            print("Tabela já existe!")
        
    def execute(self, **kwargs):
        df = self.load(**kwargs)
        self.save(df)


        