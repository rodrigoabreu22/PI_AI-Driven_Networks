import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host="localhost",
    port=8123,  # Use 8123 for HTTP connection, 9000 for Native
    username=CLICKHOUSE_USER,  # Your ClickHouse user
    password=CLICKHOUSE_PASSWORD  # Your ClickHouse password
)

# Test connection
print(client.query("SELECT version()").result_rows)