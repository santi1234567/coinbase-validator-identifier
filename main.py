import EventLogDecoder
from utils import get_last_block, write_data, write_checkpoint
import Postgres
from web3 import Web3, HTTPProvider
import requests
import time
import argparse
from tqdm import tqdm
import json
from dotenv import load_dotenv
load_dotenv()
BEACONCHAIN_CONTRACT_ADDRESS = '0x00000000219ab540356cBB839Cbe05303d7705Fa'
COINBASE_ADDRESS = '0xA090e606E30bD747d4E6245a1517EbE430F0057e'


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description="Uses eth deposits to beaconchain contract to identify coinbase validators")
    parser.add_argument(
        "--postgres", "--db", metavar="CONNECTION_STRING", type=str, help="postgres connection string. Example: postgresql://user:password@netloc:port/dbname")
    parser.add_argument("--save-in-db", action="store_true",
                        help="Save the results in the database")
    parser.add_argument("--infura-project-id",
                        metavar="INFURA_PROJECT_ID", type=str, help="Infura project id")
    parser.add_argument(
        "--covalent-api-key", metavar="COVALENT_API_KEY", type=str, help="Covalent API key")
    return parser


def parse_db_connection_string(s):
    port = s.split(':')[3].split('/')[0]
    database = s.split(':')[3].split('/')[1]
    user = s.split(':')[1].split('@')[0].split('//')[1]
    password = s.split(':')[2].split('@')[0]
    host = s.split(':')[2].split('@')[1]
    return port, database, user, password, host


if __name__ == "__main__":
    parser = create_arg_parser()
    args = parser.parse_args()
    infura_url = f'https://mainnet.infura.io/v3/{args.infura_project_id}'
    covalent_api_key = args.covalent_api_key
    postgres_endpoint = args.postgres
    try:
        port, database, user, password, host = parse_db_connection_string(
            postgres_endpoint)
    except:
        print("Invalid postgres connection string. Enter a valid connection string or set the POSTGRES_ENDPOINT environment variable.")
        exit()
    with open('contract_abi.json', 'r') as file:
        # Connect to an Ethereum node
        w3 = Web3(HTTPProvider(infura_url))

        abi = json.load(file)
        # connect to the database
        contract = w3.eth.contract(
            address=BEACONCHAIN_CONTRACT_ADDRESS, abi=abi)
        eld = EventLogDecoder.EventLogDecoder(contract)

        db = Postgres.Postgres(port=port, database=database,
                               user=user, password=password, host=host)
        if args.save_in_db:
            db.create_table("t_coinbase_validators",
                            "f_validator_pubkey bytea NOT NULL", "f_validator_pubkey")
        print("Connected to database")
        last_block = get_last_block(db, save_into_db=args.save_in_db)
        print("Last block processed:", last_block)
        try:
            contract_deposits = db.dict_query(
                f"SELECT f_eth1_sender, f_validator_pubkey, f_eth1_block_number FROM t_eth1_deposits WHERE f_eth1_sender NOT IN (SELECT f_eth1_sender FROM t_eth1_deposits GROUP BY f_eth1_sender HAVING COUNT(*) > 1) AND f_eth1_block_number > {last_block} ORDER BY f_eth1_block_number ASC;")
        except Exception as e:
            print("Error:", e)
            exit()

        validators = []
        CHECKPOINT_COUNT_AMOUNT = 100
        checkpoint_count = 0
        for row in tqdm(contract_deposits):
            while True:
                try:
                    sender = '0x'+bytes(row['f_eth1_sender']).hex()
                    request = requests.get(
                        f'https://api.covalenthq.com/v1/eth-mainnet/address/{sender}/transactions_v3/?key={covalent_api_key}')
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
                        if args.save_in_db:
                            db.insert_rows("t_coinbase_validators",
                                           "f_validator_pubkey", validators)
                        else:
                            write_data(validators)
                        validators = []
                        block_number = row['f_eth1_block_number']
                        write_checkpoint(block_number)
                        checkpoint_count = 0
                    break
                except Exception as e:
                    print('Error:', e)
                    time.sleep(10)
        db.close()
