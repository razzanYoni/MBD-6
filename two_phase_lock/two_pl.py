import utils.logger as logger
import utils.log_symbol as log_symbol
from two_phase_lock.lock_table import LockTable
from utils.action_type import Action
from utils.schedule_parser import ScheduleItem


class TwoPL:
    def __init__(self, schedule: list[ScheduleItem]):
        self.schedule = schedule
        self.transactions_queue: dict[str, list[ScheduleItem]] = {}
        self.lock_table = LockTable()
        self.transaction_order = []
        self.abort_transactions = []
        self.final_schedule = []
    
    def read_action(self, operation, check_table = False):
        if operation.transaction_id not in self.transaction_order:
            self.add_transaction_order(operation.transaction_id)

        if operation.transaction_id in self.abort_transactions:
            return False

        if self.lock_table.get_lock_type(operation.transaction_id, operation.resource) == None:
            if self.lock_table.is_unlocked(operation.resource):
                if self.is_transaction_in_queue(operation.transaction_id):
                    if not self.is_operation_in_queue(operation) and not check_table:
                        if not self.is_older_transaction(operation.transaction_id, self.lock_table.lock_table[operation.resource][0][1]):
                            self.abort_action(operation.transaction_id)
                            # self.add_transaction_queue(operation)
                        else :
                            self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                            logger.log_action(operation)
                            
                            self.final_schedule.append(operation)
                            return True

                    else :
                        self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.READ)
                        logger.log_action(operation)
                        self.final_schedule.append(operation)
                        return True
                else :
                    self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.READ)
                    logger.log_action(operation)
                    self.final_schedule.append(operation)
        else:
            if self.is_transaction_in_queue(operation.transaction_id):
                if not self.is_operation_in_queue(operation):
                    self.add_transaction_queue(operation)
                else :
                    self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.READ)
                    logger.log_action(operation)
                    self.final_schedule.append(operation)
                    return True
            else :
                logger.log_action(operation)
                self.final_schedule.append(operation)
        
        return False
    
    def write_action(self, operation, check_table = False):
        if operation.transaction_id not in self.transaction_order:
            self.add_transaction_order(operation.transaction_id)
        
        if operation.transaction_id in self.abort_transactions:
            return False

        if self.lock_table.get_lock_type(operation.transaction_id, operation.resource) == None:
            if self.lock_table.is_unlocked(operation.resource):
                if self.is_transaction_in_queue(operation.transaction_id):
                    if not self.is_operation_in_queue(operation) and not check_table:
                        if not self.is_older_transaction(operation.transaction_id, self.lock_table.lock_table[operation.resource][0][1]):
                            self.abort_action(operation.transaction_id)
                            self.add_transaction_queue(operation)
                        else :
                            self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                            logger.log_action(operation)
                            self.final_schedule.append(operation)
                            return True
                    else :
                        self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                        logger.log_action(operation)
                        self.final_schedule.append(operation)
                        return True
                else :
                    self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                    logger.log_action(operation)
                    self.final_schedule.append(operation)
            else :
                if not self.is_operation_in_queue(operation) and not check_table:
                    if not self.is_older_transaction(operation.transaction_id, self.lock_table.lock_table[operation.resource][0][1]):
                        self.abort_action(operation.transaction_id)
                        # self.add_transaction_queue(operation)
                    else :
                        self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                        logger.log_action(operation)
                        return True

                else :
                    self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                    logger.log_action(operation)
                    self.final_schedule.append(operation)
                    return True
        else:
            if self.lock_table.get_lock_type(operation.transaction_id, operation.resource) == Action.WRITE:
                if self.is_transaction_in_queue(operation.transaction_id): 
                    if not self.is_operation_in_queue(operation):
                        self.add_transaction_queue(operation)
                    else :
                        self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                        logger.log_action(operation)
                        self.final_schedule.append(operation)
                        return True
                else:
                    logger.log_action(operation)
                    self.final_schedule.append(operation)
            else:
                if self.is_operation_in_queue(operation):
                    self.add_transaction_queue(operation)
                else :
                    self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                    logger.log_action(operation)
                    self.final_schedule.append(operation)
                    return True
                
        return False
                
    def commit_action(self, operation, check_table = False):
        if operation.transaction_id in self.abort_transactions:
            return False

        if self.is_transaction_in_queue(operation.transaction_id) and not self.is_operation_in_queue(operation) and not check_table:
            self.add_transaction_queue(operation)
            return False
        else :
            logger.log_action(operation)
            self.final_schedule.append(operation)
            unlocked_resources = self.lock_table.unlock_transaction(operation.transaction_id)
            if not check_table:
                for transaction_id in self.transactions_queue:
                    found = False
                    to_pop = []
                    n_operations = len(self.transactions_queue[transaction_id])
                    for i in range(n_operations):
                        operation = self.transactions_queue[transaction_id][i]
                        if found:
                            if not self.run_operation(operation, True) :
                                break
                        else :
                            if operation.resource in unlocked_resources:
                                if self.run_operation(operation) :
                                    to_pop.append(operation)
                                    found = True
                                else :
                                    break
                            else :
                                break
                    for operation in to_pop:
                        self.transactions_queue[transaction_id].remove(operation)
                
            return False if not check_table else True
        
    def abort_action(self, transaction_id):
        if transaction_id in self.abort_transactions:
            return
        self.abort_transactions.append(transaction_id)
        logger.log(transaction_id, log_symbol.CRITICAL_SYMBOL, f'deadlock, abort T{transaction_id}')
        self.lock_table.unlock_transaction(transaction_id)
        if transaction_id in self.transaction_order:
            self.transaction_order.remove(transaction_id)
        if transaction_id in self.transactions_queue:
            del self.transactions_queue[transaction_id]

        self.final_schedule = [operation for operation in self.schedule if operation.transaction_id not in self.abort_transactions]
    
    def add_transaction_queue(self, operation):
        logger.log_add_to_queue(operation)
        if operation.transaction_id in self.transactions_queue:
            self.transactions_queue[operation.transaction_id].append(operation)
        else :
            self.transactions_queue[operation.transaction_id] = [operation]

    def is_transaction_in_queue(self, transaction_id):
        return transaction_id in self.transactions_queue

    def is_operation_in_queue(self, operation):
        return self.is_transaction_in_queue(operation.transaction_id) and self.transactions_queue[operation.transaction_id][0].action == operation.action and self.transactions_queue[operation.transaction_id][0].resource == operation.resource

    def add_transaction_order(self, transaction_id):
        self.transaction_order.append(transaction_id)
    
    def is_older_transaction(self, transaction_id, other_transaction_id):
        return self.transaction_order.index(transaction_id) < self.transaction_order.index(other_transaction_id)

    def run_operation(self, operation, check_table = False):
        if operation.action == Action.READ:
            return self.read_action(operation, check_table)
        elif operation.action == Action.WRITE:
            return self.write_action(operation, check_table)
        else: # operation == COMMIT:
            return self.commit_action(operation, check_table)

    def run(self):
        for operation in self.schedule:
            self.run_operation(operation)
        
        for transaction_id in self.abort_transactions:
            self.abort_transactions.remove(transaction_id)
            for operation in self.schedule:
                if operation.transaction_id == transaction_id:
                    self.run_operation(operation)
                
