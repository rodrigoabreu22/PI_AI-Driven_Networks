import logging
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from xgboost import XGBClassifier


NROWS = 200000


def load_csv_data(filepath):
    """Load data from a CSV file"""
    logging.info(f"Loading dataset from {filepath}...")
    df = pd.read_csv(filepath, nrows=NROWS)
    logging.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    return df

def pre_process_data(df):
    """Preprocess data to predict 'Label' (ignoring 'Attack')"""
    logging.info("Preprocessing data...")

    # Drop string columns that are not useful for training
    non_numeric_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    drop_cols = list(set(non_numeric_cols) - set(['Label']))  # keep 'Label'

    df = df.drop(columns=drop_cols, errors='ignore')

    # Drop any remaining object-type (non-numeric) columns
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                logging.warning(f"Dropping non-numeric column: {col}")
                df = df.drop(columns=[col])

    # Drop rows with NaNs or infs
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    if 'Label' not in df.columns:
        raise ValueError("'Label' column is required for classification")
    
    logging.info(f"Rows with Label = 1: {df[df['Label'] == 1].shape[0]}")

    logging.info(f"Final dataset shape after preprocessing: {df.shape}")
    return df

def train_and_compare_classifiers(df):
    """Train and evaluate classifiers to predict the 'Label' column"""
    logging.info("Training and comparing classifiers...")

    feature_cols = df.columns.difference(['Label', 'Attack'])  # Explicitly drop Attack if present
    X = df[feature_cols]
    y = df['Label']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42),
        "MLP (Neural Network)": MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42),
        "XGBoost": XGBClassifier(n_estimators=100, eval_metric='logloss', random_state=42)
    }

    models = {}

    for name, clf in classifiers.items():
        logging.info(f"Training {name}...")
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        logging.info(f"\n--- Classification Report: {name} ---")
        logging.info(f"\n{classification_report(y_test, y_pred, zero_division=0)}")
        models[name] = clf

    return models

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_logs_csv.log'),
            logging.StreamHandler()
        ]
    )

    try:
        logging.info("Starting the CSV-based ML pipeline...")
        df = load_csv_data('dataset_files/data.csv')
        df_processed = pre_process_data(df)
        train_and_compare_classifiers(df_processed)
        logging.info("Model training complete.")
    except Exception as e:
        logging.error(f"Error during training: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
