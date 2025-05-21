import logging
import numpy as np
from collections import Counter
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, matthews_corrcoef
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier #pip install xgboost
from imblearn.over_sampling import SMOTE #pip install imbalanced-learn
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline as ImbPipeline
from tabulate import tabulate #pip install tabulate
import pickle
import requests
from kafka import KafkaConsumer
import json
import pandas as pd

SMOTE_FLAG = 2  # 0 = OFF, 1 = SMOTE, 2 = SMOTE + Undersampling
SMOTE_FLAG_ATTACK = 2  # 0 = OFF, 1 = SMOTE, 2 = SMOTE + Undersampling
BROKER = 'localhost:29092'
TOPIC_PROCESSED_NETWORK_DATA = "PROCESSED_NETWORK_DATA"

def create_kafka_consumer():
    return KafkaConsumer(
        TOPIC_PROCESSED_NETWORK_DATA,
        bootstrap_servers=BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

def fetch_data_flows_loop(batch_size=5000):
    logging.info("Starting Kafka inference loop...")

    consumer = create_kafka_consumer()
    flow_buffer = []
    i=0

    for message in consumer:
        try:
            flow = message.value
            flow_buffer.append(flow)
            i+=1

            if i >= batch_size:
                logging.info(f"Processing batch of {len(flow_buffer)} flows...")

                i=0

                df = pd.DataFrame(flow_buffer)
                logging.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")

                df_processed = pre_process_data(df)
                logging.info(f"Processed batch shape: {df_processed.shape}")

                rows_with_label_1 = df[df['Label'] == 1].shape[0]
                logging.info(f"Rows with Label = 1: {df[df['Label'] == 1].shape[0]}")

                logging.info("Training and comparing classifiers...")
                df_processed_attack, label_encoder, attack_mapping = pre_process_data_attack(df)
                train_and_compare_classifiers(df_processed, smote_flag=SMOTE_FLAG)
                train_and_compare_classifiers_attack(df_processed_attack,smote_flag=SMOTE_FLAG_ATTACK, attack_mapping=attack_mapping)
                logging.info("Model training complete.")

        except Exception as e:
            logging.error(f"Error processing flow: {e}", exc_info=True)

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

def pre_process_data_attack(df):
    logging.info("Preprocessing data for the attack type...")

    non_numeric_cols = [
        'FLOW_START_MILLISECONDS', 'FLOW_END_MILLISECONDS',
        'IPV4_SRC_ADDR', 'L4_SRC_PORT', 'IPV4_DST_ADDR', 'L4_DST_PORT',
        'ICMP_TYPE', 'ICMP_IPV4_TYPE', 'DNS_QUERY_ID', 'DNS_QUERY_TYPE',    # Drop string columns not useful for training
        'DNS_TTL_ANSWER', 'FTP_COMMAND_RET_CODE', 'Attack', 'id'
    ]
    drop_cols = list(set(non_numeric_cols) - {'Attack'})
    df = df.drop(columns=drop_cols, errors='ignore')
    df = optimize_dataframe_dtypes(df)

    for col in df.columns:
        if col in ('Attack', 'Label'):
            continue
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                logging.warning(f"Dropping non-numeric column: {col}")
                df = df.drop(columns=[col])

    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    if 'Attack' not in df.columns:
        raise ValueError("'Attack' column is missing from the dataset")

    label_encoder = LabelEncoder()
    # Encode attack labels directly
    df['Attack'] = label_encoder.fit_transform(df['Attack'])
    attack_mapping = dict(zip(label_encoder.classes_, range(len(label_encoder.classes_))))
    logging.info(f"Attack labels encoded: {attack_mapping}")

    
    # Filter to only rows where Label == 1
    if 'Label' not in df.columns:
        raise ValueError("'Label' column is missing from the dataset")
    df = df[df['Label'] == 1]
    logging.info(f"Filtered dataset to only rows with Label = 1. New shape: {df.shape}")
    logging.info(f"Attack class distribution after filtering:\n{df['Attack'].value_counts()}")
    logging.info(f"Final dataset shape after preprocessing: {df.shape}")

    # Ensure all columns are shown
    pd.set_option('display.max_columns', None)

    # Ensure wide content in columns is fully shown
    pd.set_option('display.max_colwidth', None)

    # Print the full row
    print(df.sample(1))
    
    return df, label_encoder, attack_mapping

def train_and_compare_classifiers(df, smote_flag=0):
    feature_cols = df.columns.difference(['Label'])
    X = df[feature_cols]
    y = df['Label']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    if smote_flag == 1:
        smote = SMOTE(random_state=42)
        X_train, y_train = smote.fit_resample(X_train, y_train)
        logging.info("SMOTE applied.")
    elif smote_flag == 2:
        smote = SMOTE(random_state=42)
        undersample = RandomUnderSampler(random_state=42)
        pipeline = ImbPipeline([('o', smote), ('u', undersample)])
        X_train, y_train = pipeline.fit_resample(X_train, y_train)
        logging.info("SMOTE and undersampling applied.")
    elif smote_flag == 0:
        logging.info("SMOTE flag set to 0, no resampling will be applied.")

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42),
        "MLP (Neural Network)": MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42),
        "XGBoost": XGBClassifier(n_estimators=100, eval_metric='logloss', random_state=42)
    }

    results = []
    models = {}

    for name, clf in classifiers.items():
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        logging.info(f"{name} results:\n{classification_report(y_test, y_pred, zero_division=0)}\n")
        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        f1_score_value = report['macro avg']['f1-score']
        mcc = matthews_corrcoef(y_test, y_pred)
        results.append([name, f"{f1_score_value:.4f}", f"{mcc:.4f}"])
        models[name] = clf

    # Find the best model based on F1 Score
    best_model_name = max(results, key=lambda x: float(x[1]))[0]
    best_model = models[best_model_name]
    logging.info(f"The best model is: {best_model_name}")

    # Serialize model to memory (no file)
    model_bytes = pickle.dumps(best_model)
    logging.info("Best model serialized to memory.")

    deploy_model_to_inference_service(model_bytes)

    headers = ["Model", "F1 Score", "Matthews Corr Coef"]
    summary_table = tabulate(results, headers=headers, tablefmt='grid')
    logging.info("\n=== Model Performance Summary ===\n" + summary_table)

    return models

def train_and_compare_classifiers_attack(df, smote_flag=0, attack_mapping=None):
    logging.info("Training and comparing classifiers...")

    if 'Attack' not in df.columns:
        raise ValueError("'Attack' column not found in DataFrame")

    feature_cols = df.columns.difference(['Attack', 'Label']) if 'Label' in df.columns else df.columns.difference(['Attack'])
    X = df[feature_cols]
    y = df['Attack']

    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
    except ValueError as e:
        logging.warning(f"Stratified split failed: {e}. Using unstratified split instead.")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

    if smote_flag == 1:
        class_counts = Counter(y_train)
        min_class_size = min(class_counts.values())
        if min_class_size > 1:
            k_neighbors = max(1, min(min_class_size - 1, 5))
            smote = SMOTE(random_state=42, k_neighbors=k_neighbors)
            X_train, y_train = smote.fit_resample(X_train, y_train)
            logging.info(f"SMOTE applied with k_neighbors={k_neighbors}.")
        else:
            logging.warning("SMOTE skipped due to insufficient samples in at least one class.")
    elif smote_flag == 2:
        class_counts = Counter(y_train)
        min_class_size = min(class_counts.values())
        if min_class_size > 1:
            k_neighbors = max(1, min(min_class_size - 1, 5))
            smote = SMOTE(random_state=42, k_neighbors=k_neighbors)
            undersample = RandomUnderSampler(random_state=42)
            pipeline = ImbPipeline([('smote', smote), ('under', undersample)])
            X_train, y_train = pipeline.fit_resample(X_train, y_train)
            logging.info(f"SMOTE + UnderSampler applied with k_neighbors={k_neighbors}.")
        else:
            logging.warning("SMOTE + UnderSampler skipped due to insufficient samples.")
    else:
        logging.info("No resampling applied (smote_flag = 0).")

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42),
        "MLP (Neural Network)": MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42)
    }

    results = []
    models = {}

    for name, clf in classifiers.items():
        logging.info(f"Training {name}...")
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        f1_score_value = report['macro avg']['f1-score']
        mcc = matthews_corrcoef(y_test, y_pred)
        logging.info(f"\n--- Classification Report: {name} ---\n{classification_report(y_test, y_pred, zero_division=0)}")
        results.append([name, f"{f1_score_value:.4f}", f"{mcc:.4f}"])
        models[name] = clf

    best_model_name = max(results, key=lambda x: float(x[1]))[0]
    best_model = models[best_model_name]
    logging.info(f"The best model is: {best_model_name}")

    model_bytes = pickle.dumps(best_model)
    logging.info("Best model serialized to memory.")

    deploy_model_to_inference_service_attack(model_bytes)

    send_attack_mapping(attack_mapping)

    headers = ["Model", "F1 Score", "Matthews Corr Coef"]
    logging.info("\n=== Model Performance Summary ===\n" + tabulate(results, headers=headers, tablefmt='grid'))

    return models

def deploy_model_to_inference_service(model_bytes, endpoint='http://0.0.0.0:9050/update-model'):
    """Send the trained model (in memory) to the inference server."""
    logging.info(f"Deploying model to inference service at {endpoint}...")
    try:
        files = {'modeltr_pickle': ('model.pkl', model_bytes)}
        response = requests.post(endpoint, files=files)
        if response.status_code == 200:
            logging.info("Model successfully deployed to inference service.")
        else:
            logging.error(f"Failed to deploy model. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Exception during model deployment: {str(e)}")

def deploy_model_to_inference_service_attack(model_bytes, endpoint='http://0.0.0.0:9050/update-model-attack'):
    """Send the trained model (in memory) to the inference server."""
    logging.info(f"Deploying model to inference service at {endpoint}...")
    try:
        files = {'modeltr_pickle': ('model_attack.pkl', model_bytes)}
        response = requests.post(endpoint, files=files)
        if response.status_code == 200:
            logging.info("Model successfully deployed to inference service.")
        else:
            logging.error(f"Failed to deploy model. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Exception during model deployment: {str(e)}")

def send_attack_mapping(mapping, endpoint='http://0.0.0.0:9050/update-attack-mapping'):
    try:
        response = requests.post(endpoint, json=mapping)
        if response.status_code == 200:
            logging.info("Attack mapping successfully sent to inference service.")
        else:
            logging.error(f"Failed to send attack mapping. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Exception during attack mapping send: {str(e)}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_logs_csv.log'),
            logging.StreamHandler()
        ]
    )

    try:
        logging.info("Starting the ML pipeline...")
        fetch_data_flows_loop()
    except Exception as e:
        logging.error(f"Error during training: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()