import logging
import clickhouse_connect
import numpy as np
import pickle
from collections import Counter
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, matthews_corrcoef
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline as ImbPipeline
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier
from xgboost import XGBClassifier
from tabulate import tabulate

SMOTE_FLAG = 2  # Set to 0, 1, or 2

def fetch_data_flows(client):
    logging.info("Fetching new training data from ClickHouse...")
    df = client.query_df("SELECT * FROM network_data")
    logging.info(f"Fetched {len(df)} rows and {len(df.columns)} columns")
    return df

def pre_process_data(df):
    logging.info("Preprocessing data...")

    non_numeric_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    drop_cols = list(set(non_numeric_cols) - {'Attack'})
    df = df.drop(columns=drop_cols, errors='ignore')

    for col in df.columns:
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

    
    logging.info(f"Final dataset shape after preprocessing: {df.shape}")
    return df, label_encoder

def train_and_compare_classifiers(df, smote_flag=0):
    logging.info("Training and comparing classifiers...")

    if 'Attack' not in df.columns:
        raise ValueError("'Attack' column not found in DataFrame")

    feature_cols = df.columns.difference(['Attack', 'Label']) if 'Label' in df.columns else df.columns.difference(['Attack'])
    X = df[feature_cols]
    y = df['Attack']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

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
        logging.info("No resampling applied (SMOTE_FLAG = 0).")

    classifiers = {
        "Random Forest": RandomForestClassifier(n_estimators=100, random_state=42),
        "Gradient Boosting": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, random_state=42),
        "MLP (Neural Network)": MLPClassifier(hidden_layer_sizes=(100,), max_iter=300, random_state=42),
        "XGBoost": XGBClassifier(n_estimators=100, eval_metric='mlogloss', random_state=42)
    }

    results = []
    models = {}

    for name, clf in classifiers.items():
        logging.info(f"Training {name}...")
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
        weighted_f1 = report['weighted avg']['f1-score']
        mcc = matthews_corrcoef(y_test, y_pred)
        logging.info(f"\n--- Classification Report: {name} ---\n{classification_report(y_test, y_pred, zero_division=0)}")
        results.append([name, f"{weighted_f1:.4f}", f"{mcc:.4f}"])
        models[name] = clf

    best_model_name = max(results, key=lambda x: float(x[1]))[0]
    best_model = models[best_model_name]

    with open("best_classifier_model.pkl", "wb") as f:
        pickle.dump(best_model, f)
    logging.info(f"Best model '{best_model_name}' saved as 'best_classifier_model.pkl'.")

    headers = ["Model", "Weighted F1 Score", "Matthews Corr Coef"]
    logging.info("\n=== Model Performance Summary ===\n" + tabulate(results, headers=headers, tablefmt='grid'))

    return models

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_logs4.log'),
            logging.StreamHandler()
        ]
    )

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
        train_and_compare_classifiers(df_processed, smote_flag=SMOTE_FLAG)
        logging.info("Model training complete.")
    except Exception as e:
        logging.error(f"Error during training: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
