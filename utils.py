import os


def write_data(validators):
    with open('coinbase.txt', 'a') as file:
        for validator in validators:
            file.write(f'{validator}\n')


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


def get_last_block(db):
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
                data = db.dict_query(query)
                last_block = data[0]['f_eth1_block_number']
    return last_block
