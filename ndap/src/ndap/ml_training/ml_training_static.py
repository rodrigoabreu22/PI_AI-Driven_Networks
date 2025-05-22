import logging
import pandas as pd
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
import clickhouse_connect

SMOTE_FLAG = 2

def fetch_data_flows(client):
    """Fetch data from ClickHouse using clickhouse_connect."""
    logging.info("Fetching new training data from ClickHouse...")
    df = client.query_df("SELECT * FROM network_data")
    logging.info(f"Fetched {len(df)} rows and {len(df.columns)} columns")
    return df


def pre_process_data(df):
    """Preprocess data for binary classification: Benign (0) vs. Attack (1)."""
    logging.info("Preprocessing data...")

    # Columns to drop
    cols_to_drop = [
        'FLOW_START_MILLISECONDS', 'FLOW_END_MILLISECONDS',
        'IPV4_SRC_ADDR', 'L4_SRC_PORT', 'IPV4_DST_ADDR', 'L4_DST_PORT',
        'ICMP_TYPE', 'ICMP_IPV4_TYPE', 'DNS_QUERY_ID', 'DNS_QUERY_TYPE',
        'DNS_TTL_ANSWER', 'FTP_COMMAND_RET_CODE', 'Attack', 'id'
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')

    # Handle object/string columns
    non_numeric_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    drop_cols = list(set(non_numeric_cols) - set(['Label']))  # keep 'Label'
    df = df.drop(columns=drop_cols, errors='ignore')

    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = df[col].astype(float)
            except ValueError:
                logging.warning(f"Dropping non-numeric column: {col}")
                df = df.drop(columns=[col], errors='ignore')

    # Replace infs and drop NaNs
    df = df.replace([np.inf, -np.inf], np.nan).dropna()

    # Ensure Label column exists
    if 'Label' not in df.columns:
        raise ValueError("'Label' column is required for classification")

    # Convert Label if needed
    if df['Label'].dtype == 'object':
        df['Label'] = df['Label'].str.lower().map({'benign': 0, 'attack': 1})
        if df['Label'].isnull().any():
            raise ValueError("Label column contains unknown string values.")

    df['Label'] = df['Label'].astype(int)

    logging.info("Preprocessing completed.")

    # Ensure all columns are shown
    pd.set_option('display.max_columns', None)

    # Ensure wide content in columns is fully shown
    pd.set_option('display.max_colwidth', None)

    # Print the full row
    print(df.sample(10))
    return df


def train_and_compare_classifiers(df, smote_flag=1):
    feature_cols = [col for col in df.columns if col != 'Label']
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
        weighted_f1 = report['macro avg']['f1-score']
        mcc = matthews_corrcoef(y_test, y_pred)
        results.append([name, f"{weighted_f1:.4f}", f"{mcc:.4f}"])
        models[name] = clf

        # Log feature importances for tree-based models
        if hasattr(clf, 'feature_importances_'):
            importances = clf.feature_importances_
            feature_importance_data = list(zip(feature_cols, importances))
            feature_importance_data.sort(key=lambda x: x[1], reverse=True)  # Sort by importance
            top_features = feature_importance_data[:5]  # Log top 5 features
            logging.info(f"Top 5 important features for {name}:")
            for feature, importance in top_features:
                logging.info(f"{feature}: {importance:.4f}")

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
        logging.info("Starting the CSV-based ML pipeline...")
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