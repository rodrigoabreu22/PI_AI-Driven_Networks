import pandas as pd
import numpy as np
import pickle
import logging

def load_model(pickle_file='ml_training/best_model.pkl'):
    with open(pickle_file, 'rb') as f:
        model = pickle.load(f)
    return model

def preprocess_data_for_prediction(df):
    cols_to_drop = [
        'FLOW_START_MILLISECONDS', 'FLOW_END_MILLISECONDS',
        'IPV4_SRC_ADDR', 'L4_SRC_PORT', 'IPV4_DST_ADDR', 'L4_DST_PORT',
        'ICMP_TYPE', 'ICMP_IPV4_TYPE', 'DNS_QUERY_ID', 'DNS_QUERY_TYPE',
        'DNS_TTL_ANSWER', 'FTP_COMMAND_RET_CODE', 'Attack', 'Label'  # drop Label for prediction
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

def predict(model, df):
    # Ensure features are in the same order as during training
    df = df[model.feature_names_in_]
    predictions = model.predict(df)
    return predictions

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('predict_log.log'),
            logging.StreamHandler()
        ]
    )

    try:
        logging.info("Loading model...")
        model = load_model('best_model.pkl')
        logging.info("Model loaded successfully.")

        logging.info("Loading new data for prediction...")
        df = pd.read_csv('unseen_data.csv')  # <-- Update this filename if needed
        df_processed = preprocess_data_for_prediction(df)
        logging.info(f"Preprocessed prediction data shape: {df_processed.shape}")

        logging.info("Making predictions...")
        preds = predict(model, df_processed)
        logging.info(f"Predictions completed. First 10 predictions: {preds[:10]}")

        # Save predictions
        output = pd.DataFrame(preds, columns=['Prediction'])
        output.to_csv('predictions.csv', index=False)
        logging.info("Predictions saved to 'predictions.csv'.")

    except Exception as e:
        logging.error(f"Error during prediction: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
