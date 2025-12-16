from loanapproval.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from loanapproval.entity.config_entity import DataValidationConfig
from loanapproval.exception.exception import LoanApprovalException
from loanapproval.logging.logger import logging
from loanapproval.constant.training_pipeline import SCHEMA_FILE_PATH
import os,sys
import pandas as pd
from loanapproval.utils.main_utils.utils import read_yaml_file

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                data_validation_config:DataValidationConfig):
        
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise LoanApprovalException(e,sys)
        
    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            df =  pd.read_csv(file_path)
            df.columns = df.columns.str.strip()
            return df
        except Exception as e:
            raise LoanApprovalException(e,sys)
        
    def validate_number_of_columns(self,dataframe:pd.DataFrame)->bool:
        try:
            number_of_columns=len(self._schema_config['columns'])
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            else:
                return False
        except Exception as e:
            raise LoanApprovalException(e,sys)
    def is_numerical_columns_exist(self,dataframe: pd.DataFrame)->bool:
        try: 
            number_of_columns=len(self._schema_config['numerical_columns'])
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.select_dtypes('int64').columns)}")
            if len(dataframe.select_dtypes('int64').columns) == number_of_columns:
                return True
            else:
                return False 
        except Exception as e:
            raise LoanApprovalException(e,sys)
        
    def is_categorical_columns_exist(self,dataframe: pd.DataFrame)->bool:
        try: 
            number_of_columns=len(self._schema_config['categorical_columns'])
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.select_dtypes('object').columns)-1}") # We subtract 1 because there is one categorical target column
            if len(dataframe.select_dtypes('object').columns)-1 == number_of_columns:
                return True
            else:
                return False 
        except Exception as e:
            raise LoanApprovalException(e,sys)
    
        
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            error_message = ""
            validation_status = True

            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_dataframe = DataValidation.read_data(train_file_path)
            test_dataframe = DataValidation.read_data(test_file_path)

            if not self.validate_number_of_columns(train_dataframe):
                validation_status = False
                error_message += "Train dataframe does not contain all columns.\n"

            if not self.validate_number_of_columns(test_dataframe):
                validation_status = False
                error_message += "Test dataframe does not contain all columns.\n"

            if not self.is_numerical_columns_exist(train_dataframe):
                validation_status = False
                error_message += "Train dataframe missing numerical columns.\n"

            if not self.is_numerical_columns_exist(test_dataframe):
                validation_status = False
                error_message += "Test dataframe missing numerical columns.\n"

            if not self.is_categorical_columns_exist(train_dataframe):
                validation_status = False
                error_message += "Train dataframe missing categorical columns.\n"

            if not self.is_categorical_columns_exist(test_dataframe):
                validation_status = False
                error_message += "Test dataframe missing categorical columns.\n"

            if not validation_status:
                raise Exception(error_message)

            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)

            train_dataframe.to_csv(self.data_validation_config.valid_train_file_path,index=False, header=True)

            test_dataframe.to_csv(self.data_validation_config.valid_test_file_path,index=False, header=True)

            return DataValidationArtifact(
                validation_status=True,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None
            )

        except Exception as e:
            raise LoanApprovalException(e, sys)