from loanapproval.components.data_ingestion import DataIngestion
from loanapproval.entity.config_entity import TrainingPipelineConfig,DataIngestionConfig
from loanapproval.exception.exception import LoanApprovalException
from loanapproval.logging.logger import logging
import sys

if __name__=='__main__':
    try:
        trainingpipelineconfig=TrainingPipelineConfig()
        dataingestionconfig=DataIngestionConfig(trainingpipelineconfig)
        data_ingestion=DataIngestion(dataingestionconfig)
        logging.info("Initiate the data ingestion")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        logging.info("Data Initiation Completed")
        print(dataingestionartifact)
    except Exception as e:
        raise LoanApprovalException(e,sys)