import utils.logger as logger
import utils.log_symbol as log_symbol
from two_phase_lock.lock_table import LockTable
from utils.action_type import READ, WRITE, COMMIT

def twoPL(schedule):
    def read(transaction, resource):
        logger.Logger().log(transaction=transaction, symbol=log_symbol.INFO_SYMBOL, description=f"Transaction {transaction} is reading {resource}")

    def write(transaction, resource):
        logger.Logger().log(transaction=transaction, symbol=log_symbol.INFO_SYMBOL, description=f"Transaction {transaction} is writing {resource}")

    def commit(transaction):
        logger.Logger().log(transaction=transaction, symbol=log_symbol.INFO_SYMBOL, description=f"Transaction {transaction} is committing")

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
                    if lock_table.is_unlocked(resource):
                        lock_table.add_lock(transaction_id, resource, READ)
                        read(transaction_id, resource)
                else:
                    read(transaction_id, resource)
            else: # operation == WRITE:
                if lock_table.get_lock_type(transaction_id, resource) == None:
                    if lock_table.is_unlocked(resource):
                        lock_table.add_lock(transaction_id, resource, WRITE)
                        write(transaction_id, resource)
                    else :
                        lock_table.add_lock(transaction_id, resource, WRITE)
                        transactions_queue.append([transaction_id, resource])
                else:
                    if lock_table.get_lock_type(transaction_id, resource) == WRITE:
                        write(transaction_id, resource)
                    else:
                        transactions_queue.append([transaction_id, resource])

    while len(transactions_queue) > 0:
        pass
    
    logger.Logger().__str__()