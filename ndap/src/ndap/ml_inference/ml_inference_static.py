import logging
import pandas as pd
import numpy as np
import pickle
import sys

# Constants
MODEL_FILE = 'ml_training/best_model.pkl'
DATA_FILE = 'ml_inference/unseen_data.csv' 
OUTPUT_FILE = 'ml_inference/predictions.csv'

def load_model(model_path):
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    logging.info(f"Model loaded from {model_path}")
    return model

def load_new_data(filepath):
    df = pd.read_csv(filepath)
    return df

def preprocess_data_for_prediction(df):
    cols_to_drop = [
        'FLOW_START_MILLISECONDS','FLOW_END_MILLISECONDS',
        'IPV4_SRC_ADDR','L4_SRC_PORT','IPV4_DST_ADDR','L4_DST_PORT',
        'ICMP_TYPE','ICMP_IPV4_TYPE','DNS_QUERY_ID','DNS_QUERY_TYPE',
        'DNS_TTL_ANSWER','FTP_COMMAND_RET_CODE','Label','Attack'
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
    non_numeric_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    df = df.drop(columns=non_numeric_cols, errors='ignore')

    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                df = df.drop(columns=[col])

    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    return df

def predict_and_save(model, df, original_df):
    predictions = model.predict(df)
    original_df['Prediction'] = predictions
    original_df.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Predictions saved to {OUTPUT_FILE}")

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )

    try:
        logging.info("Loading model and new data for prediction...")
        model = load_model(MODEL_FILE)
        raw_df = load_new_data(DATA_FILE)
        processed_df = preprocess_data_for_prediction(raw_df.copy())
        predict_and_save(model, processed_df, raw_df)
        logging.info("Prediction complete.")
    except Exception as e:
        logging.error(f"Error during prediction: {str(e)}", exc_info=True)

if __name__ == '__main__':
    main()
