import logging
import clickhouse_connect
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, matthews_corrcoef
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from xgboost import XGBClassifier
import pickle
from imblearn.over_sampling import SMOTE #pip install imbalanced-learn
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline as ImbPipeline
from tabulate import tabulate #pip install tabulate

SMOTE_FLAG = 0  # 0 = OFF, 1 = SMOTE, 2 = SMOTE + Undersampling

def fetch_data_flows(client):
    """Fetch data from ClickHouse using clickhouse_connect."""
    logging.info("Fetching new training data from ClickHouse...")
    df = client.query_df("SELECT * FROM network_data")
    logging.info(f"Fetched {len(df)} rows and {len(df.columns)} columns")
    return df

def pre_process_data(df):
    cols_to_drop = [
        'FLOW_START_MILLISECONDS','FLOW_END_MILLISECONDS',
        'IPV4_SRC_ADDR','L4_SRC_PORT','IPV4_DST_ADDR','L4_DST_PORT',
        'ICMP_TYPE','ICMP_IPV4_TYPE','DNS_QUERY_ID','DNS_QUERY_TYPE',
        'DNS_TTL_ANSWER','FTP_COMMAND_RET_CODE','Attack'
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')
    non_numeric_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    drop_cols = list(set(non_numeric_cols) - set(['Label']))
    df = df.drop(columns=drop_cols, errors='ignore')

    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                df = df.drop(columns=[col])

    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    if 'Label' not in df.columns:
        raise ValueError("'Label' column is required for classification")

    return df


def train_and_compare_classifiers(df, smote_flag=2):
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
        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        logging.info(f"\n--- Classification Report: {name} ---")
        logging.info(f"\n{classification_report(y_test, y_pred, zero_division=0)}")
        weighted_f1 = report['macro avg']['f1-score']
        mcc = matthews_corrcoef(y_test, y_pred)
        results.append([name, f"{weighted_f1:.4f}", f"{mcc:.4f}"])
        models[name] = clf

    # Find the best model based on Weighted F1 Score
    best_model_name = max(results, key=lambda x: float(x[1]))[0]
    best_model = models[best_model_name]
    logging.info(f"The best model is: {best_model_name}")

    # Save the best model to a pickle file
    with open('best_model.pkl', 'wb') as f:
        pickle.dump(best_model, f)
    logging.info("Best model saved as 'best_model.pkl'.")

    headers = ["Model", "Weighted F1 Score", "Matthews Corr Coef"]
    summary_table = tabulate(results, headers=headers, tablefmt='grid')
    logging.info("\n=== Model Performance Summary ===\n" + summary_table)

    return models


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_logs3.log'),
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
        df_processed = pre_process_data(df)
        logging.info(f"Rows with Label = 1: {df[df['Label'] == 1].shape[0]}")
        train_and_compare_classifiers(df_processed)
        logging.info("Model training complete.")
    except Exception as e:
        logging.error(f"Error during training: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()