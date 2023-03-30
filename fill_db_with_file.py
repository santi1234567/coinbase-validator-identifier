import argparse
import Postgres
import re
from tqdm import tqdm
# Function to chunk a list into batches of a given size


def chunk_list(lst, size):
    return [lst[i:i+size] for i in range(0, len(lst), size)]


def parse_db_connection_string(s):
    port = s.split(':')[3].split('/')[0]
    database = s.split(':')[3].split('/')[1]
    user = s.split(':')[1].split('@')[0].split('//')[1]
    password = s.split(':')[2].split('@')[0]
    host = s.split(':')[2].split('@')[1]
    return port, database, user, password, host


parser = argparse.ArgumentParser()
parser.add_argument(
    "--postgres", "--db", metavar="CONNECTION_STRING", type=str, help="postgres connection string. Example: postgresql://user:password@netloc:port/dbname")

args = parser.parse_args()


try:
    port, database, user, password, host = parse_db_connection_string(
        args.postgres)
except:
    print("Invalid postgres connection string. Enter a valid connection string or set the POSTGRES_ENDPOINT environment variable.")
    exit()

db = Postgres.Postgres(port=port, database=database,
                       user=user, password=password, host=host)

db.create_table("t_coinbase_validators",
                "f_validator_pubkey bytea NOT NULL", "f_validator_pubkey")
validators = []

with open("backup/coinbase.txt", "r") as f:
    validators = f.readlines()
print(f"Found {len(validators)} validators")
# Chunk the validators list into batches of 1000
validator_batches = chunk_list(validators, 1000)

for batch in tqdm(validator_batches):
    db.insert_rows("t_coinbase_validators", "f_validator_pubkey", batch)

db.close()
