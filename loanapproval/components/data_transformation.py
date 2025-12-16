import sys
import os 
import pandas as pd 
import numpy as np
from sklearn.pipeline import Pipeline 
from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer
from sklearn.compose import ColumnTransformer
from loanapproval.constant.training_pipeline import SCHEMA_FILE_PATH
from loanapproval.utils.main_utils.utils import read_yaml_file
from loanapproval.constant.training_pipeline import TARGET_COLUMN
from loanapproval.entity.artifact_entity import DataTransformationArtifact, DataValidationArtifact
from loanapproval.entity.config_entity import DataTransformationConfig
from loanapproval.exception.exception import LoanApprovalException
from loanapproval.logging.logger import logging 
from loanapproval.utils.main_utils.utils import save_numpy_array_data, save_object, sqrt_transform

class DataTransformation:
    def __init__(self,data_validation_artifact:DataValidationArtifact,
                data_transformation_config:DataTransformationConfig):
        try:
            self.data_validation_artifact:DataValidationArtifact=data_validation_artifact
            self.data_transformation_config:DataTransformationConfig=data_transformation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise LoanApprovalException(e,sys)
        
    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.strip()
            return df
        except Exception as e:
            raise LoanApprovalException(e, sys)
        
    def get_data_transformer_object(self):
        
        try:
            num_cols = self._schema_config["numerical_columns"]
            cat_cols = self._schema_config["categorical_columns"]

            sqrt_transformer = FunctionTransformer(sqrt_transform,validate=False)

            num_pipeline = Pipeline(steps=[("sqrt", sqrt_transformer),("scaler", StandardScaler())])

            cat_pipeline = Pipeline(steps=[("onehotencoder", OneHotEncoder())])

            preprocessor = ColumnTransformer(
                transformers=[
                    ("num", num_pipeline, num_cols),
                    ("cat", cat_pipeline, cat_cols)
                ]
            )

            return preprocessor
        except Exception as e:
            raise LoanApprovalException(e,sys)


        
    def initiate_data_transformation(self)->DataTransformationArtifact:
        logging.info("Entered initiate_data_transformation method of DataTransformation class")
        try:
            logging.info("Starting data transformation")
            train_df=DataTransformation.read_data(self.data_validation_artifact.valid_train_file_path)
            test_df=DataTransformation.read_data(self.data_validation_artifact.valid_test_file_path)

            ## training dataframe
            input_feature_train_df=train_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN]
            target_feature_train_df = target_feature_train_df.replace({' Approved':0,' Rejected':1})
            target_feature_train_df = target_feature_train_df.astype(int)

            ## testing dataframe
            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN]
            target_feature_test_df = target_feature_test_df.replace({' Approved':0,' Rejected':1})
            target_feature_test_df = target_feature_test_df.astype(int)
                
            preprocessor=self.get_data_transformer_object()

            preprocessor_object=preprocessor.fit(input_feature_train_df)
            transformed_input_train_feature=preprocessor_object.transform(input_feature_train_df)
            transformed_input_test_feature =preprocessor_object.transform(input_feature_test_df)
            

            train_arr = np.c_[transformed_input_train_feature, np.array(target_feature_train_df) ]
            test_arr = np.c_[ transformed_input_test_feature, np.array(target_feature_test_df) ]

            #save numpy array data
            save_numpy_array_data( self.data_transformation_config.transformed_train_file_path, array=train_arr, )
            save_numpy_array_data( self.data_transformation_config.transformed_test_file_path,array=test_arr,)
            save_object( self.data_transformation_config.transformed_object_file_path, preprocessor_object,)


            #preparing artifacts

            data_transformation_artifact=DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
            )
            return data_transformation_artifact


        except Exception as e:
            raise LoanApprovalException(e,sys)
