# dq_engine.py
import pandas as pd
from datetime import datetime

# ----------------------------
# DATA QUALITY SCORING
# ----------------------------
txn_df = pd.read_csv("transactions.csv")
cust_df = pd.read_csv("customer_kyc.csv")
merch_df = pd.read_csv("merchant_master.csv")

def completeness_score(df):
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isnull().sum().sum()
    score = 100 * (1 - missing_cells / total_cells)
    return round(score, 2)
print("Completeness (Transactions):", completeness_score(txn_df))

def uniqueness_score(df):
    unique_ratio = df[id_column].nunique() / len(df)
    return round(unique_ratio * 100, 2)

print("Uniqueness (Transactions):", uniqueness_score(txn_df, "transaction_id"))

VALID_CURRENCIES = ["INR", "USD", "EUR"]

def validity_score_currency(df):
    invalid = ~df["currency_code"].isin(VALID_CURRENCIES)
    score = 100 * (1 - invalid.mean())
    return round(score, 2)

print("Validity (Currency):", validity_score_currency(txn_df))

def accuracy_score_amount(df):
    invalid_amounts = (df["transaction_amount"] <= 0) | (df["transaction_amount"].isnull())
    score = 100 * (1 - invalid_amounts.mean())
    return round(score, 2)
print("Accuracy (Amount):", accuracy_score_amount(txn_df))

def timeliness_score(df, max_delay_days=7):
    df["transaction_timestamp"] = pd.to_datetime(df["transaction_timestamp"])
    df["settlement_date"] = pd.to_datetime(df["settlement_date"])

    delay = (df["settlement_date"] - df["transaction_timestamp"]).dt.days
    late = delay > max_delay_days

    score = 100 * (1 - late.mean())
    return round(score, 2)

print("Timeliness:", timeliness_score(txn_df))


def consistency_score(df):
    inconsistent = (
        (df["currency_code"] == "INR") & (df["merchant_id"].isnull())
    )
    score = 100 * (1 - inconsistent.mean())
    return round(score, 2)
print("Consistency:", consistency_score(txn_df))

def integrity_score(txn_df, cust_df, merch_df):
    invalid_customer = ~txn_df["customer_id"].isin(cust_df["customer_id"])
    invalid_merchant = ~txn_df["merchant_id"].isin(merch_df["merchant_id"])

    invalid = invalid_customer | invalid_merchant
    score = 100 * (1 - invalid.mean())
    return round(score, 2)

print("Integrity:", integrity_score(txn_df, cust_df, merch_df))


# ----------------------------
# GENAI EXPLANATION LAYER
# ----------------------------

def explain_dimension(dimension, score):
    if score >= 90:
        severity = "low"
    elif score >= 75:
        severity = "medium"
    else:
        severity = "high"

    explanations = {
        "Completeness": {
            "low": "Most required fields are populated, indicating strong data capture processes.",
            "medium": "Some critical fields contain missing values, which may affect downstream processing.",
            "high": "A significant number of required fields are missing, impacting reconciliation and compliance."
        },
        "Accuracy": {
            "low": "Transaction values are largely accurate and within expected ranges.",
            "medium": "Some transaction values appear unrealistic or invalid, affecting reporting reliability.",
            "high": "Many transaction values are incorrect or invalid, posing financial and operational risks."
        },
        "Validity": {
            "low": "Most fields conform to expected formats and domain rules.",
            "medium": "Several fields violate format or domain constraints, reducing system reliability.",
            "high": "Widespread format and domain violations prevent reliable data processing."
        },
        "Uniqueness": {
            "low": "Records are mostly unique, minimizing duplication risks.",
            "medium": "Some duplicate records were detected, which may lead to reconciliation errors.",
            "high": "High duplication levels detected, risking double counting and reporting inaccuracies."
        },
        "Timeliness": {
            "low": "Transactions are settled within acceptable timeframes.",
            "medium": "Settlement delays are observed, impacting real-time visibility.",
            "high": "Significant delays detected, reducing operational effectiveness and fraud detection capability."
        },
        "Consistency": {
            "low": "Related fields show strong alignment across records.",
            "medium": "Some inconsistencies exist between related fields, causing reporting mismatches.",
            "high": "Frequent inconsistencies detected, reducing trust in analytical outputs."
        },
        "Integrity": {
            "low": "Relationships across datasets are well maintained.",
            "medium": "Some records reference missing or invalid entities.",
            "high": "Broken relationships detected, impacting end-to-end data reliability."
        }
    }

    return explanations[dimension][severity]

def generate_recommendation(dimension, score):
    if score >= 85:
        return "No immediate action required. Continue monitoring."
    
    recommendations = {
        "Completeness": "Enforce mandatory field validation at data ingestion.",
        "Accuracy": "Apply value range checks and validation rules at the source.",
        "Validity": "Standardize formats and apply domain-level validations.",
        "Uniqueness": "Implement unique constraints and deduplication logic.",
        "Timeliness": "Optimize settlement workflows and monitor delays.",
        "Consistency": "Introduce cross-field validation rules.",
        "Integrity": "Enforce referential integrity across related datasets."
    }

    return recommendations[dimension]

# ----------------------------
# MAIN PIPELINE
# ----------------------------

def analyze_dataset(txn_df, cust_df, merch_df):
    scores = {
        "Completeness": completeness_score(txn_df),
        "Accuracy": accuracy_score_amount(txn_df),
        "Validity": validity_score_currency(txn_df),
        "Uniqueness": uniqueness_score(txn_df, "transaction_id"),
        "Timeliness": timeliness_score(txn_df),
        "Consistency": consistency_score(txn_df),
        "Integrity": integrity_score(txn_df, cust_df, merch_df)
    }

    explanations = {
        k: explain_dimension(k, v)
        for k, v in scores.items()
    }

    recommendations = {
        k: generate_recommendation(k, v)
        for k, v in scores.items()
    }

    overall_dqs = round(sum(scores.values()) / len(scores), 2)

    return {
        "overall_dqs": overall_dqs,
        "scores": scores,
        "explanations": explanations,
        "recommendations": recommendations
    }
