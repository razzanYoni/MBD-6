import utils.logger as logger
import utils.log_symbol as log_symbol
import two_phase_lock.lock_state as lock_state

class LockTable:
    def __init__(self):
        self.lock_table = {}

    ##
    # @brief      Adds a lock to the lock table.
    #
    # @param      self          The object
    # @param      transaction   The transaction
    # @param      resource      The resource
    # @param      lock_type     The lock type
    #
    def add_lock(self, transaction, resource, lock_type):
        if resource not in self.lock_table:
            self.lock_table[resource] = [[lock_type, transaction]]
        else:
            self.lock_table[resource].append([lock_type, transaction])

        logger.Logger().log(transaction=transaction, symbol=log_symbol.INFO_SYMBOL, description=f"Added lock: {lock_type} on {resource} for {transaction}")

    ##
    # @brief      Unlocks a resource.
    #
    # @param      self        The object
    # @param      resource    The resource
    # @param      transaction The transaction
    #
    def unlock(self, transaction, resource):
        if len(self.lock_table[resource]) > 1 :
            self.lock_table[resource].remove(transaction)
        else:
            del self.lock_table[resource]
        logger.Logger().log(transaction=transaction, symbol=log_symbol.INFO_SYMBOL, description=f"Unlocked {resource} for {transaction}")

    ##
    # @brief      Unlocks a transaction.
    #
    # @param      self         The object
    # @param      transaction  The transaction
    #
    def unlock_transaction(self, transaction):
        logger.Logger().log(transaction=transaction, symbol=log_symbol.INFO_SYMBOL, description=f"Unlocked all resources for {transaction}")
        for resource in self.lock_table:
            if transaction in self.lock_table[resource]:
                self.unlock(transaction, resource)

    ##
    # @brief    Is unlocked
    #
    # @param      self      The object
    # @param      resource  The resource
    #
    def is_unlocked(self, resource):
        return resource not in self.lock_table

    ##
    # @brief      Is transaction has a valid read lock
    #
    # @param      self         The object
    # @param      transaction  The transaction
    # @param      resource     The resource
    #
    def is_read_locked(self, transaction, resource):
        if resource not in self.lock_table:
            return False

        for lt, tx in self.lock_table[resource]:
            if lt == lock_state.X :
                if tx == transaction :
                    return True
                else :
                    return False
            else :
                if tx == transaction :
                    return True
        
        return False

    ##
    # @brief      Is transaction has a valid write lock
    #
    # @param      self         The object
    # @param      transaction  The transaction
    # @param      resource     The resource
    #
    def is_write_locked(self, transaction, resource):
        if resource not in self.lock_table:
            return False

        for lt, tx in self.lock_table[resource]:
            if tx == transaction :
                if lt == lock_state.X :
                    return True
                else :
                    self.upgrade_lock(transaction, resource)
                    return True
            else :
                return False
    
    ##
    # @brief      Gets the lock type.
    #
    # @param      self         The object
    # @param      transaction  The transaction
    # @param      resource     The resource
    #
    def get_lock_type(self, transaction, resource):
        if self.is_read_locked(transaction, resource):
            return lock_state.S
        elif self.is_write_locked(transaction, resource):
            return lock_state.X
        else:
            return None

    ##
    # @brief      Upgrade lock
    #
    # @param      self         The object
    # @param      transaction  The transaction
    # @param      resource     The resource
    #
    def upgrade_lock(self, transaction, resource):
        self.lock_table[resource].remove([lock_state.S, transaction])
        self.lock_table[resource].insert(0, [lock_state.X, transaction])
        logger.Logger().log(transaction=transaction, symbol=log_symbol.INFO_SYMBOL, description=f"Upgraded lock on {resource} for {transaction}")
