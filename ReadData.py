from pyspark.sql.functions import *
from connect import *
from Transform import *


class read_data:
    def __init__(self,dbnm,tblnm,tgt_dir):
        self.dbnm=dbnm
        self.tblnm=tblnm
        self.tgt_dir = tgt_dir # changed dir to tgt_dir
        self.sp=connect()
    def read_df(self):
        df = connect.resultDf(self.sp,self.dbnm,self.tblnm)
        resultDf = Transform.getMinMaxDate(self,df)
        Transform.saveAs(resultDf,self.tgt_dir)

if __name__ == "__main__":
    dbname = input("Enter DB name : ")
    tblnm = input("Enter tbl name : ")
    dir = input("Enter complete directory : ")
    #dbname = "Test"
    #tblnm = "stockdata"
    #dir = 'C:/'
    r=read_data(dbname,tblnm,dir + '/')
    r.read_df()
