from flask import Flask, render_template, request
import pandas as pd
import numpy as np
import os
import sys
import pickle

from loanapproval.exception.exception import LoanApprovalException
from loanapproval.logging.logger import logging
from loanapproval.constant.training_pipeline import SAVED_MODEL_DIR, MODEL_FILE_NAME

app = Flask(__name__)

MODEL_PATH = os.path.join("final_model", "model.pkl")
PREPROCESSOR_PATH = os.path.join("final_model", "preprocessor.pkl")


def load_object(file_path):
    try:
        with open(file_path, "rb") as file:
            return pickle.load(file)
    except Exception as e:
        raise LoanApprovalException(e, sys)


@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    try:
        data = {
            "education": request.form["education"],
            "self_employed": request.form["self_employed"],
            "dependents": int(request.form["dependents"]),
            "income_annum": float(request.form["income_annum"]),
            "loan_amount": float(request.form["loan_amount"]),
            "loan_term": float(request.form["loan_term"]),
            "cibil_score": float(request.form["cibil_score"]),
            "residential_score": float(request.form["residential_score"]),
            "commercial_assets_value": float(request.form["commercial_assets_value"]),
            "luxury_assets_value": float(request.form["luxury_assets_value"]),
            "bank_assets_value": float(request.form["bank_assets_value"])
        }

        input_df = pd.DataFrame([data])

        preprocessor = load_object(PREPROCESSOR_PATH)
        model = load_object(MODEL_PATH)

        transformed_data = preprocessor.transform(input_df)
        prediction = model.predict(transformed_data)[0]

        result = "Loan Approved ✅" if prediction == 0 else "Loan Rejected ❌"

        return render_template("result.html", prediction=result)

    except Exception as e:
        logging.error(e)
        return render_template("result.html", prediction="Error Occurred")


if __name__ == "__main__":
    app.run(debug=True)
