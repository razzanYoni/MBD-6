import utils.log_symbol as log_symbol
import utils.logger as logger
from two_phase_lock.lock_table import LockTable
from utils.action_type import Action
from utils.schedule_parser import ScheduleItem


class TwoPL:
    def __init__(self, schedule: list[ScheduleItem]):
        self.schedule = schedule
        self.transactions_queue = {}
        self.lock_table = LockTable()
        self.transaction_order = []
    
    def read_action(self, operation):
        if operation.transaction_id in self.transaction_order:
            self.add_transaction_queue(operation)

        if self.lock_table.get_lock_type(operation.transaction_id, operation.resource) == None:
            if self.lock_table.is_unlocked(operation.resource):
                if self.is_transaction_in_queue(operation.transaction_id):
                    self.add_transaction_queue(operation)
                else :
                    self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.READ)
                    self.read(operation.transaction_id, operation.resource)
        else:
            if self.is_transaction_in_queue(operation.transaction_id):
                self.add_transaction_queue(operation)
            else :
                self.read(operation.transaction_id, operation.resource)
    
    def write_action(self, operation):
        if operation.transaction_id in self.transaction_order:
            self.add_transaction_queue(operation)

        if self.lock_table.get_lock_type(operation.transaction_id, operation.resource) == None:
            if self.lock_table.is_unlocked(operation.resource):
                if self.is_transaction_in_queue(operation.transaction_id):
                    self.add_transaction_queue(operation)
                else :
                    self.lock_table.add_lock(operation.transaction_id, operation.resource, Action.WRITE)
                    self.write(operation.transaction_id, operation.resource)
            else :
                self.add_transaction_queue(operation)
        else:
            if self.lock_table.get_lock_type(operation.transaction_id, operation.resource) == Action.WRITE:
                if self.is_transaction_in_queue(operation.transaction_id):
                    self.add_transaction_queue(operation)
                else:
                    self.write(operation.transaction_id, operation.resource)
            else:
                self.add_transaction_queue(operation)

    def commit_action(self, operation):
        if self.is_transaction_in_queue(operation.transaction_id):
            self.add_transaction_queue(operation)
        else :
            self.commit(operation.transaction_id)
            self.lock_table.unlock_transaction(operation.transaction_id)

    def add_transaction_queue(self, operation):
        if operation.transaction_id in self.transactions_queue:
            self.transactions_queue[operation.transaction_id].append([operation])
        else :
            self.transactions_queue[operation.transaction_id] = [[operation]]

    def is_transaction_in_queue(self, transaction_id):
        return transaction_id in self.transactions_queue

    def is_operation_in_queue(self, operation):
        return self.is_transaction_in_queue(operation.transaction_id) and self.transactions_queue[operation.transaction_id][0][0].action == operation.action and self.transactions_queue[operation.transaction_id][0][0].resource == operation.resource

    def add_transaction_order(self, transaction_id):
        self.transaction_order.append(transaction_id)

    def resolve(self):
        for operation in self.schedule:
            if operation.action == Action.COMMIT:
                self.commit_action(operation)
            else:
                if operation.action == Action.READ:
                    self.read_action(operation)
                else: # operation == WRITE:
                    self.write_action(operation)
