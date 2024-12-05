from pyspark.sql import functions
from pyspark.sql import SparkSession
import configparser
class connect:
      def __init__(self):
            try :
                  self.confP = configparser.RawConfigParser()
                  self.confP.read("config.cfg")
                  self.sp = (SparkSession
                        .builder
                        .config('spark.jars', self.confP.get('Section','postgres_jar_path') +'postgresql-42.7.0.jar')
                        .master('local')
                        .appName('Test')
                        .getOrCreate())
                  
            except :
                  print("Found error")

      def resultDf(self,dbname,tblnm):
            try:
                  df = (self.sp.read
                        .format('jdbc')
                        .option('url', "jdbc:postgresql://localhost:5432/" + dbname)
                        .option('dbtable', tblnm)
                        .option('user', self.confP.get('Section','user'))
                        .option('password', self.confP.get('Section','password'))
                        .option('driver', 'org.postgresql.Driver')
                        .load()
                        )

                  return df
            except ValueError as error:
                  print("Found error",error.value)