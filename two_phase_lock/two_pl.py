import utils.logger as logger
import utils.log_symbol as log_symbol
from lock_table import LockTable
from utils.action_type import READ, WRITE, COMMIT
import queue

def twoPL(schedule):
    def read(transaction, resource):
        logger.log(symbol=log_symbol.INFO_SYMBOL, description=f"Transaction {transaction} is reading {resource}")

    def write(transaction, resource):
        logger.log(symbol=log_symbol.INFO_SYMBOL, description=f"Transaction {transaction} is writing {resource}")

    def commit(transaction):
        logger.log(symbol=log_symbol.INFO_SYMBOL, description=f"Transaction {transaction} is committing")

    transactions_queue = []
    lock_table = LockTable()

    for operation in schedule:
        if operation[0] == COMMIT:
            action, transaction_id = operation
            commit(transaction_id)
            lock_table.unlock_transaction(transaction_id)
        else:
            action, transaction_id, resource = operation
            if action == READ:
                if lock_table.get_lock_type(transaction_id, resource) == None:
                    pass
                else:
                    read(transaction_id, resource)
            else: # operation == WRITE:
                if lock_table.get_lock_type(transaction_id, resource) == None:
                    pass
                elif lock_table.get_lock_type(transaction_id, resource) == WRITE:
                    write(transaction_id, resource)
                else:
                    pass
