import utils.logger as logger
import utils.log_symbol as log_symbol
import lock_state

class LockTable:
    def __init__(self):
        self.lock_table = {}

    def add_lock(self, transaction, resource, lock_type):
        if resource not in self.lock_table:
            self.lock_table[resource] = [lock_type, transaction]
        else:
            self.lock_table[resource].append([lock_type, transaction])

        logger.log(symbol=log_symbol.INFO_SYMBOL, description=f"Added lock: {lock_type} on {resource} for {transaction}")

    def unlock(self, transaction, resource):
        if len(self.lock_table[resource]) > 1 :
            self.lock_table[resource].remove(transaction)
        else:
            del self.lock_table[resource]
        logger.log(symbol=log_symbol.INFO_SYMBOL, description=f"Unlocked {resource} for {transaction}")

    def unlock_transaction(self, transaction):
        for resource in self.lock_table:
            if transaction in self.lock_table[resource]:
                self.unlock(transaction, resource)

    def is_unlocked(self, resource):
        return resource not in self.lock_table

    def is_read_locked(self, transaction, resource):
        return [lock_state.READ_LOCKED, transaction] in self.lock_table[resource]

    def is_write_locked(self, transaction, resource):
        return [lock_state.WRITE_LOCKED, transaction] in self.lock_table[resource]
    
    def get_lock_type(self, transaction, resource):
        if self.is_read_locked(transaction, resource):
            return lock_state.READ_LOCKED
        elif self.is_write_locked(transaction, resource):
            return lock_state.WRITE_LOCKED
        else:
            return None

    def upgrade_lock(self, transaction, resource):
        self.lock_table[resource].remove([lock_state.READ_LOCKED, transaction])
        self.lock_table[resource].append([lock_state.WRITE_LOCKED, transaction])
        logger.log(symbol=log_symbol.INFO_SYMBOL, description=f"Upgraded lock on {resource} for {transaction}")