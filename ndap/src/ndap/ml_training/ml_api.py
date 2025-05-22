from sklearn.metrics import f1_score, accuracy_score, classification_report, matthews_corrcoef
import clickhouse_connect
import pandas as pd
import logging
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from xgboost import XGBClassifier
import numpy as np


def optimize_dataframe_dtypes(df):
    dtype_mapping = {
        'PROTOCOL': 'uint8',
        'L7_PROTO': 'float32',
        'IN_BYTES': 'uint32',
        'IN_PKTS': 'uint32',
        'OUT_BYTES': 'uint32',
        'OUT_PKTS': 'uint32',
        'TCP_FLAGS': 'uint8',
        'CLIENT_TCP_FLAGS': 'uint8',
        'SERVER_TCP_FLAGS': 'uint8',
        'FLOW_DURATION_MILLISECONDS': 'uint32',
        'DURATION_IN': 'uint32',
        'DURATION_OUT': 'uint32',
        'MIN_TTL': 'uint8',
        'MAX_TTL': 'uint8',
        'LONGEST_FLOW_PKT': 'uint16',
        'SHORTEST_FLOW_PKT': 'uint16',
        'MIN_IP_PKT_LEN': 'uint16',
        'MAX_IP_PKT_LEN': 'uint16',
        'RETRANSMITTED_IN_BYTES': 'uint32',
        'RETRANSMITTED_IN_PKTS': 'uint32',
        'RETRANSMITTED_OUT_BYTES': 'uint32',
        'RETRANSMITTED_OUT_PKTS': 'uint32',
        'SRC_TO_DST_AVG_THROUGHPUT': 'uint64',
        'DST_TO_SRC_AVG_THROUGHPUT': 'uint64',
        'NUM_PKTS_UP_TO_128_BYTES': 'uint16',
        'NUM_PKTS_128_TO_256_BYTES': 'uint16',
        'NUM_PKTS_256_TO_512_BYTES': 'uint16',
        'NUM_PKTS_512_TO_1024_BYTES': 'uint16',
        'NUM_PKTS_1024_TO_1514_BYTES': 'uint16',
        'TCP_WIN_MAX_IN': 'uint32',
        'TCP_WIN_MAX_OUT': 'uint32',
        'Label': 'uint8',
        'Attack': 'string'
    }

    for col, dtype in dtype_mapping.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                logging.warning(f"Could not convert column {col} to {dtype}: {e}")
    
    return df

def pre_process_data(df):
    """Preprocess data for binary classification: Benign (0) vs. Attack (1)."""
    logging.info("Preprocess data for binary classification: Benign (0) vs. Attack (1)...")
    # Drop string columns not useful for training
    non_numeric_cols = [
        'FLOW_START_MILLISECONDS', 'FLOW_END_MILLISECONDS',
        'IPV4_SRC_ADDR', 'L4_SRC_PORT', 'IPV4_DST_ADDR', 'L4_DST_PORT',
        'ICMP_TYPE', 'ICMP_IPV4_TYPE', 'DNS_QUERY_ID', 'DNS_QUERY_TYPE',    # Drop string columns not useful for training
        'DNS_TTL_ANSWER', 'FTP_COMMAND_RET_CODE', 'Attack', 'id'
    ]
    logging.info(non_numeric_cols)
    drop_cols = list(set(non_numeric_cols))

    df = df.drop(columns=drop_cols, errors='ignore')
    df = optimize_dataframe_dtypes(df)

    # Convert object columns to numeric if possible, drop if not
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                logging.warning(f"Dropping non-numeric column: {col}")
                df = df.drop(columns=[col])

    # Drop rows with missing values
    df = df.replace([np.inf, -np.inf], np.nan).dropna()
    
    return df

def train_model_by_id(model_id, row_count):
    model_map = {
        0: ("Random Forest", RandomForestClassifier(n_estimators=100, random_state=42)),
        1: ("Gradient Boosting", GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42)),
        2: ("MLP (Neural Network)", MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42)),
        3: ("XGBoost", XGBClassifier(n_estimators=100, eval_metric='logloss', random_state=42))
    }

    if model_id not in model_map:
        logging.error(f"Invalid model_id: {model_id}. Must be 0, 1, 2, or 3.")
        return {"error": "Invalid model ID"}

    try:
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='network',
            password='network25pi',
            database='default'
        )
        query = f"SELECT * FROM network_data LIMIT {row_count}"
        logging.info(f"Fetching {row_count} rows from ClickHouse...")
        df = client.query_df(query)
    except Exception as e:
        logging.error("Failed to fetch data from ClickHouse", exc_info=True)
        return {"error": "Failed to fetch data"}

    try:
        df_processed = pre_process_data(df)
        if df_processed.empty:
            logging.error("No data left after preprocessing.")
            return {"error": "Empty dataframe after preprocessing."}

        feature_cols = df_processed.columns.difference(['Label'])
        X = df_processed[feature_cols]
        y = df_processed['Label']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

        model_name, model = model_map[model_id]
        logging.info(f"Training model: {model_name}")
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        mcc = matthews_corrcoef(y_test, y_pred)
        accuracy = accuracy_score(y_test, y_pred)

        metrics = {
            "model": model_name,
            "accuracy": accuracy,
            "precision": report['macro avg']['precision'],
            "recall": report['macro avg']['recall'],
            "f1_score": report['macro avg']['f1-score'],
            "mcc": mcc
        }

        logging.info(f"Model performance metrics: {metrics}")
        return metrics

    except Exception as e:
        logging.error("Error during model training or evaluation", exc_info=True)
        return {"error": "Training/evaluation failed"}

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_logs_csv.log'),
            logging.StreamHandler()
        ]
    )

    # Example usage
    model_id = 0  # Random Forest
    row_count = 20000  # Number of rows to fetch
    result = train_model_by_id(model_id, row_count)
    print(result)

if __name__ == "__main__":
    main()