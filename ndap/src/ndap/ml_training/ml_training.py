import logging
import clickhouse_connect
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

def fetch_data_flows(client):
    """Fetch data from ClickHouse using clickhouse_connect which automatically handles UUID conversion"""
    logging.info("Fetching new training data")
    df = client.query_df("SELECT * FROM network_data LIMIT 10000")
    return df

def pre_process_data(df):
    """Preprocess the data for machine learning"""
    # Encode categorical variables
    label_encoder = LabelEncoder()
    if 'Attack' in df.columns:
        df['Attack'] = label_encoder.fit_transform(df['Attack'])

    # Drop non-numeric or irrelevant columns
    cols_to_drop = ['IPV4_SRC_ADDR', 'IPV4_DST_ADDR']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

    # Handle missing values if any
    df = df.dropna()
    return df

def random_forest_training(df):
    """Train and evaluate a Random Forest classifier"""
    if 'Attack' not in df.columns:
        raise ValueError("'Attack' column not found in DataFrame")
        
    # Define features and target variable
    X = df.drop(columns=['Label', 'Attack'] if 'Label' in df.columns else ['Attack'])
    y = df['Attack']
    
    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Train model
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    
    # Evaluate
    y_pred = clf.predict(X_test)
    print(classification_report(y_test, y_pred))
    
    return clf

def main():
    # Initialize ClickHouse client using clickhouse_connect
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,  # Default HTTP port for clickhouse_connect
        username='network',
        password='network25pi',
        database='default'
    )

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ml_logs.log'),
            logging.StreamHandler()  # Also log to console
        ]
    )
    
    try:
        logging.info("Starting ML Model Training")
        df = fetch_data_flows(client)
        df_processed = pre_process_data(df)
        model = random_forest_training(df_processed)
        logging.info("Model training completed successfully")
    except Exception as e:
        logging.error(f"Error during model training: {str(e)}")
        raise

if __name__ == "__main__":
    main()