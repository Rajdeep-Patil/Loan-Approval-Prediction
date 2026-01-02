from flask import Flask, render_template, request
import pandas as pd
import pickle
import os
import sys
from pymongo import MongoClient
from loanapproval.utils.main_utils.utils import load_object

MONGO_URI = "mongodb+srv://rajdeepgurjar07_db_user:JJDy58TzdRamjajo@loanapproval.wkgz25u.mongodb.net/?appName=loanapproval"
client = MongoClient(MONGO_URI)

db = client["loan_approval_db"]
collection = db["predictions"]


app = Flask(__name__)

MODEL_PATH = os.path.join("final_model", "model.pkl")
PREPROCESSOR_PATH = os.path.join("final_model", "preprocessor.pkl")


@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")


@app.route("/predict", methods=["POST"])
def predict():
    try:
        print("FORM DATA 👉", request.form)

        data = {
            "education": request.form.get("education"),
            "self_employed": request.form.get("self_employed"),
            "no_of_dependents": int(request.form.get("no_of_dependents")),
            "income_annum": float(request.form.get("income_annum")),
            "loan_amount": float(request.form.get("loan_amount")),
            "loan_term": float(request.form.get("loan_term")),
            "cibil_score": float(request.form.get("cibil_score")),
            "residential_assets_value": float(request.form.get("residential_assets_value")),
            "commercial_assets_value": float(request.form.get("commercial_assets_value")),
            "luxury_assets_value": float(request.form.get("luxury_assets_value")),
            "bank_asset_value": float(request.form.get("bank_asset_value"))
        }

        df = pd.DataFrame([data])

        preprocessor = load_object(PREPROCESSOR_PATH)
        model = load_object(MODEL_PATH)

        transformed = preprocessor.transform(df)
        prediction = model.predict(transformed)[0]

        result = "Loan Approved" if prediction == 0 else "Loan Rejected"
        
        mongo_data = {
            **data,
            "prediction": result,
        }

        collection.insert_one(mongo_data)

        return render_template("result.html", prediction=result)

    except Exception as err:
        print("ERROR:", err)
        return render_template(
            "result.html",
            prediction=f"Prediction Error: {err}"
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)


