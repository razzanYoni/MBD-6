import utils.log_symbol as log_symbol
import utils.logger as logger
from utils.action_type import Action
from utils.schedule_parser import ScheduleItem
from .lock_table import LockTable, LockType
from .transaction import Transaction


class TwoPL:
    def __init__(self, schedule: list[ScheduleItem]):
        self.schedule: list[ScheduleItem] = schedule
        self.transactions: list[Transaction] = []
        self.lock_table: LockTable = LockTable()
        self.final_schedule: list[ScheduleItem] = []

    def get_t_by_id(self, t_id: str) -> Transaction | None:
        for t in self.transactions:
            if t.id == t_id:
                return t
        return None

    def get_t_idx_by_id(self, t_id: str) -> int:
        for i in range(0, len(self.transactions)):
            t = self.transactions[i]
            if t.id == t_id:
                return i
        return -1

    def is_t_older(self, t_id: str, other_t_id: str):
        id1 = -1
        id2 = -1

        for i in range(0, len(self.transactions)):
            t = self.transactions[i]
            if t.id == t_id:
                id1 = i
            if t.id == other_t_id:
                id2 = i

            if id1 != -1 and id2 != -1:
                break
        return id1 < id2

    def is_t_abort(self, t_id: str):
        t = self.get_t_by_id(t_id)
        return t.is_abort()

    def is_t_has_queue(self, t_id: str):
        t = self.get_t_by_id(t_id)
        return t.is_has_queue()

    def add_operation_to_t_queue(self, schedule_item: ScheduleItem):
        other_t_id = None
        for lock_item in self.lock_table.get_lock_item_by_resource(schedule_item.resource):
            if lock_item.t_id != schedule_item.transaction_id:
                other_t_id = lock_item.t_id

        # is other t id has the lock too
        if other_t_id is not None:
            other_t_idx = self.get_t_idx_by_id(other_t_id)
            # is younger and has queue
            if not self.is_t_older(schedule_item.transaction_id, other_t_id) and self.transactions[
                other_t_idx].is_has_queue():
                # abort transaction
                t_idx = self.get_t_idx_by_id(schedule_item.transaction_id)
                self.transactions[t_idx].abort = True
                self.transactions[t_idx].queue = []

                logger.log(self.transactions[t_idx].id, log_symbol.INFO_SYMBOL,
                           f'T{self.transactions[t_idx].id} trying to get lock, but T{other_t_id} is waiting for the lock from T{self.transactions[t_idx].id}')
                logger.log(self.transactions[t_idx].id, log_symbol.CRITICAL_SYMBOL,
                           f'Deadlock, abort T{self.transactions[t_idx].id}')

                unlocked_resource, unlock_schedule_items = self.lock_table.unlock_transaction(
                    schedule_item.transaction_id)

                i = 0
                j = 0
                while j < len(self.final_schedule):
                    scheduleitem = self.final_schedule[i]
                    if scheduleitem.transaction_id == schedule_item.transaction_id:
                        self.final_schedule.remove(scheduleitem)
                        i -= 1
                    j += 1
                    i += 1

                self.run_queue(unlocked_resource)
            else:
                # older or the older has not the queue
                t_idx = self.get_t_idx_by_id(schedule_item.transaction_id)
                self.transactions[t_idx].queue.append(schedule_item)
                logger.log_add_to_queue(schedule_item)
        else:
            # not younger
            t_idx = self.get_t_idx_by_id(schedule_item.transaction_id)
            self.transactions[t_idx].queue.append(schedule_item)
            logger.log_add_to_queue(schedule_item)

    def read(self, schedule_item: ScheduleItem, queue_action: bool = False) -> bool:
        # is t has lock on resource
        if self.lock_table.is_t_has_lock(schedule_item.transaction_id, schedule_item.resource):
            # is t still in queue
            if self.is_t_has_queue(schedule_item.transaction_id):
                # action from queue
                if queue_action:
                    self.final_schedule.append(schedule_item)
                    logger.log_action(schedule_item)
                    return True
                else:  # add to t queue
                    self.add_operation_to_t_queue(schedule_item)
            else:  # t isn't in queue
                self.final_schedule.append(schedule_item)
                logger.log_action(schedule_item)
        else:
            # is t still in queue
            if self.is_t_has_queue(schedule_item.transaction_id):
                if queue_action:
                    # other transaction has not the key
                    if not self.lock_table.is_t_has_X_lock(schedule_item.transaction_id, schedule_item.resource):
                        self.lock_table.add_lock(schedule_item.transaction_id, schedule_item.resource, LockType.S)
                else:  # add to t queue
                    self.add_operation_to_t_queue(schedule_item)
            else:
                # t isn't in queue but other transaction has the key
                if self.lock_table.is_other_t_has_X_key(schedule_item.transaction_id, schedule_item.resource):
                    self.add_operation_to_t_queue(schedule_item)
                # t is free to act
                else:
                    self.final_schedule.append(self.lock_table.add_lock(schedule_item.transaction_id, schedule_item.resource, LockType.S))
                    self.final_schedule.append(schedule_item)
                    logger.log_action(schedule_item)

        return False

    def write(self, schedule_item: ScheduleItem, queue_action: bool = False) -> bool:
        # is t has X lock on resource
        if self.lock_table.is_t_has_X_lock(schedule_item.transaction_id, schedule_item.resource):
            # is t still in queue
            if self.is_t_has_queue(schedule_item.transaction_id):
                # action from queue
                if queue_action:
                    self.final_schedule.append(schedule_item)
                    logger.log_action(schedule_item)
                    return True
                else:  # add to t queue
                    self.add_operation_to_t_queue(schedule_item)
            else:  # t isn't in queue
                self.final_schedule.append(schedule_item)
                logger.log_action(schedule_item)
        else:
            # is t still in queue
            if self.is_t_has_queue(schedule_item.transaction_id):
                # action from queue
                if queue_action:
                    if not self.lock_table.is_other_t_has_key(schedule_item.transaction_id, schedule_item.resource):
                        if self.lock_table.is_t_has_S_lock(schedule_item.transaction_id, schedule_item.resource):
                            self.final_schedule.append(self.lock_table.upgrade_lock(schedule_item.transaction_id, schedule_item.resource))
                        else:
                            self.final_schedule.append(self.lock_table.add_lock(schedule_item.transaction_id, schedule_item.resource, LockType.X))
                        self.final_schedule.append(schedule_item)
                        logger.log_action(schedule_item)
                        return True
                else:
                    # add to queue
                    self.add_operation_to_t_queue(schedule_item)
            else:
                # t isn't in queue but other transaction has key
                if self.lock_table.is_other_t_has_key(schedule_item.transaction_id, schedule_item.resource):
                    self.add_operation_to_t_queue(schedule_item)
                # t is free to act
                else:
                    if self.lock_table.is_t_has_S_lock(schedule_item.transaction_id, schedule_item.resource):
                        self.final_schedule.append(self.lock_table.upgrade_lock(schedule_item.transaction_id, schedule_item.resource))
                    else:
                        self.final_schedule.append(self.lock_table.add_lock(schedule_item.transaction_id, schedule_item.resource, LockType.X))
                    self.final_schedule.append(schedule_item)
                    logger.log_action(schedule_item)

    def commit(self, schedule_item: ScheduleItem, queue_action: bool = False) -> bool:
        # is t still in queue
        if self.is_t_has_queue(schedule_item.transaction_id):
            # action from queue
            if queue_action:
                self.final_schedule.append(schedule_item)
                logger.log_action(schedule_item)
                unlocked_resource, unlock_schedule_items = self.lock_table.unlock_transaction(
                    schedule_item.transaction_id)
                return True
            # add t to queue
            else:
                self.add_operation_to_t_queue(schedule_item)
        else:
            # t isn't in queue
            self.final_schedule.append(schedule_item)
            logger.log_action(schedule_item)
            unlocked_resource, unlock_schedule_items = self.lock_table.unlock_transaction(schedule_item.transaction_id)
            self.final_schedule += unlock_schedule_items
            t = self.get_t_by_id(schedule_item.transaction_id)
            t.queue = []
            self.run_queue(unlocked_resource)
    def run_op(self, schedule_item: ScheduleItem, queue_action: bool = False, final: bool = False) -> bool:
        if not self.get_t_by_id(schedule_item.transaction_id):
            self.transactions.append(Transaction(schedule_item.transaction_id))
        if not queue_action and not final:
            t_idx = self.get_t_idx_by_id(schedule_item.transaction_id)
            self.transactions[t_idx].schedules.append(schedule_item)

        if self.is_t_abort(schedule_item.transaction_id):
            return False

        if schedule_item.action == Action.READ:
            return self.read(schedule_item, queue_action)
        elif schedule_item.action == Action.WRITE:
            return self.write(schedule_item, queue_action)
        else:
            return self.commit(schedule_item, queue_action)

    def run_queue(self, unlocked_resource: list[str]):
        # if not queue_action:
        # check is there any operation can be done
        for i in range(0, len(self.transactions)):
            t = self.transactions[i]
            if not t.is_has_queue():
                continue
            do_next_op = False
            to_pop = 0
            for scheduleitem in t.queue:
                if do_next_op:
                    do_next_op = self.run_op(scheduleitem, queue_action=True)
                    if do_next_op:
                        to_pop += 1
                elif scheduleitem.resource in unlocked_resource:
                    do_next_op = self.run_op(scheduleitem, queue_action=True)
                    to_pop += 1

                if not do_next_op: break

            for _ in range(to_pop):
                self.transactions[i].queue.pop(0)

    def run(self):
        for schedule_item in self.schedule:
            self.run_op(schedule_item)

        for t in self.transactions:
            if t.is_abort():
                self.final_schedule += t.schedules
                t.abort = False
                for schedule_item in t.schedules:
                    self.run_op(schedule_item, final=True)
