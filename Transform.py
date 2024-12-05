from pyspark.sql.functions import *
from pyspark.sql import *
from ReadData import *
import sys,os,shutil

class Transform:
    def __init__(self):
        pass
    
    def getMinMaxDate(self,df):
        dfMinMax = df.groupBy(df.index.alias("idx"))\
                     .agg(min(col("date")).alias("MinDate"),max(col("date")).alias("MaxDate"))\
                     .orderBy("index") \
        #spark.sql.analyzer.failAmbiguousSelfJoin = False
        dfMinClose = dfMinMax.alias("a").join(df.alias("b"),(dfMinMax.idx == df.index) & (dfMinMax.MinDate == df.date),'inner')\
                             .select(col('a.idx'),col('a.MinDate'),col('a.MaxDate'),col('b.close').alias("MinClose"))

        dfMinMaxClose = dfMinClose.alias("a").join(df.alias("b"), (dfMinClose.idx == df.index) & (dfMinClose.MaxDate == df.date),'inner') \
                             .select(col('a.idx'), col('a.MinDate'), col('a.MaxDate'), col('a.MinClose'), col('b.close').alias("MaxClose"))
        
        return dfMinMaxClose
    def saveAs(df,dir):
        try:
            if os.path.exists(dir + "/Final_Transform_Op") :
                for f in os.listdir(dir):
                    if f == "Final_Transform_Op":
                        shutil.rmtree(os.path.join(dir, f))  #help to remove directory
                        break

            df.write\
                .option("header",True)\
                .csv(dir + "/Final_Transform_Op")
            print("Transformed data saved successfully at dir : ",dir + "/Final_Transform_Op")
        except OSError as error:
            print("Found error while saving data => ",error)
    


