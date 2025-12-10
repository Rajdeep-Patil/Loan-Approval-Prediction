import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL=os.getenv("MONGODB_URL_KEY")
print(MONGO_DB_URL)

import certifi
ca=certifi.where()

import pandas as pd
import numpy as np
import pymongo 
from loanapproval.exception.exception import LoanApprovalException
from loanapproval.logging.logger import logging 


class LoanApprovalExtract():
    logging.info("Error")
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise LoanApprovalException(e,sys)
    
    def csv_to_json_convertor(self,file_path):
        try:
            logging.info("Error")
            df=pd.read_csv(file_path)
            df.drop('loan_id',axis=1,inplace=True)
            df.reset_index(drop=True,inplace=True)
            records=list(json.loads(df.T.to_json()).values())
            return records
        except Exception as e:
            raise LoanApprovalException(e,sys)
        
    def insert_data_mongodb(self,records,database,collection):
        try:
            logging.info("Error")
            self.database=database
            self.collection=collection
            self.records=records
            
            self.mongo_client=pymongo.MongoClient(MONGO_DB_URL)
            self.database=self.mongo_client[self.database]
            self.collection=self.database[self.collection]
            self.collection.insert_many(self.records)
            
            return len(self.records)
        except Exception as e:
            raise LoanApprovalException(e,sys)
            
if __name__=="__main__":
    FILE_PATH="D:\Loan_Approval_Prediction\LoanApproval_Data\loan_approval_prediction.csv"
    DATABASE="Rajdeep"
    Collection="LoanApprovalData"
    networkobj=LoanApprovalExtract()
    records=networkobj.csv_to_json_convertor(file_path=FILE_PATH)
    print(records)
    no_of_records=networkobj.insert_data_mongodb(records,DATABASE,Collection)
    print(no_of_records)
    