import numpy as np 
import pandas as pd 
import os

def load_data(filepath: str) -> pd.DataFrame:
    """
    Load telecom churn dataset.
    """
    return pd.read_csv(filepath)

df = load_data("../data/telco_churn.csv")

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean raw telecom data:
    - Remove whitespace from column names
    - Convert 'TotalCharges' to numeric
    - Drop missing or invalid rows
    """
    df.columns = df.columns.str.strip()

    # Handle TotalCharges column
    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')

    # Drop rows with missing TotalCharges (after coercion)
    df = df.dropna(subset=['TotalCharges'])
    
    #removing customer IDs
    df.drop(columns=['customerID'],inplace=True)

    #converting the target to binary
    df['Churn'] = df['Churn'].map({'Yes': 1, 'No': 0})


    #one hot encoding the categorical variables
    df = pd.get_dummies(df)
    
    return df

def save_processed_data(df: pd.DataFrame, output_path: str):
    """
    Save the cleaned and processed dataset to a CSV file.

    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

def run_etl(input_path: str, output_path: str):
    """
    ETL pipeline runner.
    """
    try:
        df = load_data(input_path)
        df_cleaned = clean_data(df)
        save_processed_data(df_cleaned, output_path)
        print(f"ETL pipeline complete. Processed file saved to: {output_path}")
    except Exception as e:
        print(f"ETL pipeline failed: {e}")
