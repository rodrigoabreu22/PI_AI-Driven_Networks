import logging
import clickhouse_connect
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, matthews_corrcoef
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from xgboost import XGBClassifier #pip install xgboost
from imblearn.over_sampling import SMOTE #pip install imbalanced-learn
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline as ImbPipeline
from tabulate import tabulate #pip install tabulate
import pickle
import requests

SMOTE_FLAG = 0  # 0 = OFF, 1 = SMOTE, 2 = SMOTE + Undersampling

def fetch_data_flows(client):
    """Fetch data from ClickHouse using clickhouse_connect."""
    logging.info("Fetching new training data from ClickHouse...")
    df = client.query_df("SELECT * FROM network_data")
    logging.info(f"Fetched {len(df)} rows and {len(df.columns)} columns")
    return df

def pre_process_data(df):
    """Preprocess data for binary classification: Benign (0) vs. Attack (1)."""
    logging.info("Preprocessing data...")

    # Drop string columns not useful for training
    non_numeric_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    drop_cols = list(set(non_numeric_cols) - set(['Attack']))  # Keep 'Attack' only

    df = df.drop(columns=drop_cols, errors='ignore')

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

    # Binary encode the 'Attack' column
    if 'Attack' in df.columns:
        df['Attack'] = df['Attack'].apply(lambda x: 0 if str(x).lower() == 'benign' else 1)
        logging.info("Encoded Attack column: 0 = benign, 1 = attack")
    else:
        raise ValueError("'Attack' column is missing from the dataset")

    logging.info(f"Rows labeled as benign (0): {df[df['Attack'] == 0].shape[0]}")
    logging.info(f"Rows labeled as attack (1): {df[df['Attack'] == 1].shape[0]}")
    logging.info(f"Final dataset shape after preprocessing: {df.shape}")
    
    return df

def train_and_compare_classifiers(df, smote_flag=0):
    feature_cols = df.columns.difference(['Label'])
    X = df[feature_cols]
    y = df['Label']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

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
        weighted_f1 = report['weighted avg']['f1-score']
        mcc = matthews_corrcoef(y_test, y_pred)
        results.append([name, f"{weighted_f1:.4f}", f"{mcc:.4f}"])
        models[name] = clf

    # Find the best model based on Weighted F1 Score
    best_model_name = max(results, key=lambda x: float(x[1]))[0]
    best_model = models[best_model_name]
    logging.info(f"The best model is: {best_model_name}")

    # Serialize model to memory (no file)
    model_bytes = pickle.dumps(best_model)
    logging.info("Best model serialized to memory.")

    deploy_model_to_inference_service(model_bytes)

    headers = ["Model", "Weighted F1 Score", "Matthews Corr Coef"]
    summary_table = tabulate(results, headers=headers, tablefmt='grid')
    logging.info("\n=== Model Performance Summary ===\n" + summary_table)

    return models


def deploy_model_to_inference_service(model_bytes, endpoint='http://0.0.0.0:9050/update-model'):
    """Send the trained model (in memory) to the inference server."""
    logging.info(f"Deploying model to inference service at {endpoint}...")
    try:
        files = {'model_pickle': ('model.pkl', model_bytes)}
        response = requests.post(endpoint, files=files)
        if response.status_code == 200:
            logging.info("Model successfully deployed to inference service.")
        else:
            logging.error(f"Failed to deploy model. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Exception during model deployment: {str(e)}")


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
        client = clickhouse_connect.get_client(
            host='localhost',
            port=8123,
            username='network',
            password='network25pi',
            database='default'
        )
    except Exception as e:
        logging.error(f"Error initializing clickhouse client: {str(e)}", exc_info=True)

    try:
        logging.info("Starting the ML pipeline...")
        df = fetch_data_flows(client)
        logging.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")
        logging.info("Preprocessing data...")
        df_processed = pre_process_data(df)
        logging.info(f"Rows with Label = 1: {df[df['Label'] == 1].shape[0]}")
        logging.info(f"Final dataset shape after preprocessing: {df_processed.shape}")
        logging.info("Training and comparing classifiers...")
        train_and_compare_classifiers(df_processed, smote_flag=SMOTE_FLAG)
        logging.info("Model training complete.")
    except Exception as e:
        logging.error(f"Error during training: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()