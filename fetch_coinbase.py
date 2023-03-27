import EventLogDecoder
from web3 import Web3, HTTPProvider
import os
import requests
import time
import binascii
import psycopg2
import sys
import argparse
from tqdm import tqdm
import json
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
load_dotenv()
INFURA_URL = f'https://mainnet.infura.io/v3/{os.environ.get("INFURA_PROJECT_ID")}'
API_KEY = os.environ.get("C_KEY")
BEACONCHAIN_CONTRACT_ADDRESS = '0x00000000219ab540356cBB839Cbe05303d7705Fa'
COINBASE_ADDRESS = '0xA090e606E30bD747d4E6245a1517EbE430F0057e'

# function to convert bytea data to string


def data_to_str(s):
    if s is None:
        return None
    if isinstance(s, bytes):
        return binascii.hexlify(s).decode('utf-8')
    return str(s)


def get_last_block(conn):
    last_block = 0
    if os.path.isfile('checkpoint.txt'):
        with open('checkpoint.txt', 'r') as file:
            lines = file.readlines()
            last_line = lines[-1]
            print("found checkpoint:", last_line.strip())
            last_block = int(last_line.strip())
    elif os.path.isfile('coinbase.txt'):
        with open('coinbase.txt', 'r') as file:
            lines = file.readlines()
            if lines:
                last_line = lines[-1]
                print("Last validator found:", last_line.strip())
                query = f"SELECT f_eth1_block_number FROM t_eth1_deposits WHERE f_validator_pubkey = '{last_line.strip()}';"
                data = dict_query(conn, query)
                last_block = data[0]['f_eth1_block_number']
    return last_block


def write_data(validators):
    with open('coinbase.txt', 'a') as file:
        for validator in validators:
            file.write(f'{validator}\n')


def dict_query(conn, sql):
    # create a cursor with DictCursor and custom functions
    cursor = conn.cursor(cursor_factory=DictCursor)
    psycopg2.extensions.register_adapter(bytes, data_to_str)

    # execute the SELECT statement
    cursor.execute(
        sql)
    data = [dict(row) for row in cursor.fetchall()]
    # close the cursor and connection
    cursor.close()

    # return all the data from the table as dictionaries
    return data

# unused
# def get_known_validators():
#     import os

#     directory = "./validators/"  # Replace with the directory path you want to read

#     validators = set()  # Create an empty set to store the lines

#     for filename in os.listdir(directory):
#         filepath = os.path.join(directory, filename)
#         with open(filepath, "r") as file:
#             for line in file:
#                 # print(repr(line.strip()))
#                 validators.add(line.strip())

#     return validators


def write_checkpoint(block_number):
    with open('checkpoint.txt', 'w') as file:
        file.write(str(block_number))


def connect_database(port, name, user, password):
    conn = psycopg2.connect(
        port=port,
        database=name,
        user=user,
        password=password
    )
    return conn


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description="Uses eth deposits to beaconchain contract to identify coinbase validators")
    parser.add_argument(
        "--postgres", "--db", metavar="CONNECTION_STRING", type=str, help="postgres connection string. Example: postgresql://user:password@netloc:port/dbname")
    return parser


def parse_db_connection_string(s):
    port = s.split(':')[3].split('/')[0]
    database = s.split(':')[3].split('/')[1]
    user = s.split(':')[1].split('@')[0].split('//')[1]
    password = s.split(':')[2].split('@')[0]
    return port, database, user, password


if __name__ == "__main__":
    parser = create_arg_parser()
    args = parser.parse_args()
    port, database, user, password = parse_db_connection_string(
        args.postgres)
    conn = psycopg2.connect(
        port=port,
        database=database,
        user=user,
        password=password
    )
    print("Connected to database")
    with open('contract_abi.json', 'r') as file:
        # Connect to an Ethereum node
        w3 = Web3(HTTPProvider(INFURA_URL))

        abi = json.load(file)
        # connect to the database
        contract = w3.eth.contract(
            address=BEACONCHAIN_CONTRACT_ADDRESS, abi=abi)
        eld = EventLogDecoder.EventLogDecoder(contract)

        last_block = get_last_block(conn)

        contract_deposits = dict_query(
            conn, f"SELECT f_eth1_sender, f_validator_pubkey, f_eth1_block_number FROM t_eth1_deposits WHERE f_eth1_sender NOT IN (SELECT f_eth1_sender FROM t_eth1_deposits GROUP BY f_eth1_sender HAVING COUNT(*) > 1) AND f_eth1_block_number > {last_block} ORDER BY f_eth1_block_number ASC;")

        validators = []
        # known_validators = get_known_validators()
        CHECKPOINT_COUNT_AMOUNT = 100
        checkpoint_count = 0
        for row in tqdm(contract_deposits):
            # if "\\"+bytes(row['f_validator_pubkey']).hex() not in known_validators:
            while True:
                try:
                    sender = '0x'+bytes(row['f_eth1_sender']).hex()
                    request = requests.get(
                        f'https://api.covalenthq.com/v1/eth-mainnet/address/{sender}/transactions_v3/?key={API_KEY}')
                    transactions = request.json()['data']['items']
                    is_coinbase = False
                    tx_count_to_beaconchain_contract = 0
                    pub_key = ''
                    log_entry = ''
                    for tx in transactions:
                        if tx['to_address']:
                            if tx['to_address'].lower() == BEACONCHAIN_CONTRACT_ADDRESS.lower() and tx['successful'] == True:
                                tx_count_to_beaconchain_contract += 1
                                for event in tx['log_events']:
                                    if event['decoded']['name'] == "DepositEvent":
                                        log_entry = {
                                            'topics': event['raw_log_topics'],
                                            'data': event['raw_log_data']
                                        }
                                        decoded_log = eld.decode_log(log_entry)
                                        pub_key = decoded_log['pubkey'].hex()
                            if tx['to_address'].lower() == COINBASE_ADDRESS.lower() and tx['successful'] == True:
                                is_coinbase = True
                    if tx_count_to_beaconchain_contract != 1:
                        is_coinbase = False
                    if is_coinbase:
                        validators.append(f'\\x{pub_key}')
                    checkpoint_count += 1
                    if checkpoint_count >= CHECKPOINT_COUNT_AMOUNT or row == contract_deposits[-1]:
                        write_data(validators)
                        validators = []
                        block_number = row['f_eth1_block_number']
                        write_checkpoint(block_number)
                        checkpoint_count = 0
                    break
                except Exception as e:
                    print('Error:', e)
                    time.sleep(10)
            # else:
        #   print('Known validator', "\\" +
        #        bytes(row['f_validator_pubkey']).hex())
        conn.close()
