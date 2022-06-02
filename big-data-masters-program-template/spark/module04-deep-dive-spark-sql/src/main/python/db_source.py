class DBSource:
    def __init__(self, driver_name, connection_url, spark, username=None, password=None):
        self.driver_name = driver_name
        self.connection_url = connection_url
        self.username = username
        self.password = password
        self.spark = spark

    def get_df_from_sqlite(self, table_name):
        return self.spark.read.jdbc(url=self.connection_url, table=table_name, properties={"driver": self.driver_name})

    def get_df_from_mysql(self, table_name):
        return self.spark.read.jdbc(url=self.connection_url, table=table_name,
                                    properties={"driver": self.driver_name, "user": "root", "password": "npntraining"})
