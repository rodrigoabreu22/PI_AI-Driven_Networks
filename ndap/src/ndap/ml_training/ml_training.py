import logging
import clickhouse_connect
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier

def fetch_data_flows(client):
    """Fetch data from ClickHouse using clickhouse_connect."""
    logging.info("Fetching new training data from ClickHouse...")
    df = client.query_df("SELECT * FROM network_data")
    logging.info(f"Fetched {len(df)} rows and {len(df.columns)} columns")
    return df

def pre_process_data(df):
    """Preprocess data for Random Forest classification"""
    logging.info("Preprocessing data...")
    
    # Drop string columns that are not useful for training
    non_numeric_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    non_features = ['IPV4_SRC_ADDR', 'IPV4_DST_ADDR', 'Attack']  # Attack will be encoded, others dropped
    drop_cols = list(set(non_numeric_cols) - set(['Attack']))  # keep 'Attack' for encoding
    
    df = df.drop(columns=drop_cols, errors='ignore')

    # Drop columns with UUIDs or unparseable types if they exist
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                logging.warning(f"Dropping non-numeric column: {col}")
                df = df.drop(columns=[col])

    # Drop rows with missing values
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    # Encode target variable
    label_encoder = LabelEncoder()
    if 'Attack' in df.columns:
        df['Attack'] = label_encoder.fit_transform(df['Attack'])
        logging.info(f"Attack labels encoded: {list(label_encoder.classes_)}")
    else:
        raise ValueError("'Attack' column is missing from the dataset")
    
    logging.info(f"Rows with Label = 1: {df[df['Label'] == 1].shape[0]}")

    logging.info(f"Final dataset shape after preprocessing: {df.shape}")
    return df, label_encoder

def random_forest_training(df):
    """Train and evaluate Random Forest classifier"""
    logging.info("Training Random Forest Classifier...")

    if 'Attack' not in df.columns:
        raise ValueError("'Attack' column not found in DataFrame")
    
    # Separate features and target
    feature_cols = df.columns.difference(['Attack', 'Label']) if 'Label' in df.columns else df.columns.difference(['Attack'])
    X = df[feature_cols]
    y = df['Attack']

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train classifier
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)

    # Evaluate model
    y_pred = clf.predict(X_test)
    print("Classification Report:\n", classification_report(y_test, y_pred))

    return clf


def train_and_compare_classifiers(df):
    """
    Train and evaluate Random Forest, Gradient Boosting, and MLP classifiers.
    """
    logging.info("Training and comparing classifiers...")

    if 'Attack' not in df.columns:
        raise ValueError("'Attack' column not found in DataFrame")

    feature_cols = df.columns.difference(['Attack', 'Label']) if 'Label' in df.columns else df.columns.difference(['Attack'])
    X = df[feature_cols]
    y = df['Attack']

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced'),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42),
        "MLP (Neural Network)": MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42)
    }

    models = {}

    for name, clf in classifiers.items():
        logging.info(f"Training {name}...")
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        logging.info(f"\n--- Classification Report: {name} ---")
        logging.info(classification_report(y_test, y_pred, zero_division=0))
        models[name] = clf

    return models


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_logs.log'),
            logging.StreamHandler()
        ]
    )

    # Connect to ClickHouse
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='network',
        password='network25pi',
        database='default'
    )

    try:
        logging.info("Starting the ML pipeline...")
        df = fetch_data_flows(client)
        df_processed, label_encoder = pre_process_data(df)
        train_and_compare_classifiers(df_processed)
        #model = random_forest_training(df_processed)
        logging.info("Model training complete.")
    except Exception as e:
        logging.error(f"Error during training: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
