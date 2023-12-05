import utils.log_symbol as log_symbol
import utils.logger as logger
from utils.action_type import Action
from utils.schedule_parser import ScheduleItem
from .lock_table import LockTable, LockType
from .transaction import TransactionManager, TransactionStatus


# rigorous two phase locking
class TwoPL:
    def __init__(self, schedule: list[ScheduleItem]):
        self.schedule: list[ScheduleItem] = schedule
        self.transaction_manager: TransactionManager = TransactionManager()
        self.lock_table: LockTable = LockTable()
        self.final_schedule: list[ScheduleItem] = []

    def add_operation_to_t_queue(self, schedule_item: ScheduleItem):
        other_t_id = None
        for lock_item in self.lock_table.get_lock_item_by_resource(schedule_item.resource):
            if lock_item.t_id != schedule_item.transaction_id:
                other_t_id = lock_item.t_id

        # is other t id has the lock too
        if other_t_id is not None:
            # is transaction younger and waiting (wait-die scheme)
            if not self.transaction_manager.is_t_older(schedule_item.transaction_id, other_t_id) \
                    and \
                    self.transaction_manager.is_t_waiting(other_t_id):
                # abort transaction
                t = self.transaction_manager.get_t_by_id(schedule_item.transaction_id)
                t.status = TransactionStatus.ABORTED
                t.queue = []

                logger.log(t.id, log_symbol.INFO_SYMBOL,
                           f'T{t.id} is trying to get lock, but T{other_t_id} is waiting for the lock from T{t.id}')
                logger.log(t.id, log_symbol.CRITICAL_SYMBOL,
                           f'Deadlock, abort T{t.id}')

                unlocked_resource, unlock_schedule_items = self.lock_table.unlock_transaction(
                    schedule_item.transaction_id)

                i = 0
                j = 0
                n_final_schedule = len(self.final_schedule)
                while j < n_final_schedule:
                    scheduleitem = self.final_schedule[i]
                    if scheduleitem.transaction_id == schedule_item.transaction_id:
                        self.final_schedule.pop(i)
                        i -= 1
                    j += 1
                    i += 1

                self.run_waiting_queue(unlocked_resource)
                return

        # older or waiting or not younger
        t = self.transaction_manager.get_t_by_id(schedule_item.transaction_id)
        t.status = TransactionStatus.WAITING
        if schedule_item not in t.queue: t.add_queue(schedule_item)
        logger.log_add_to_queue(schedule_item)

    def read(self, schedule_item: ScheduleItem, queue_action: bool = False) -> bool:
        # is t has lock on resource
        if self.lock_table.is_t_has_lock(schedule_item.transaction_id, schedule_item.resource):
            # is t still waiting
            if self.transaction_manager.is_t_waiting(schedule_item.transaction_id):
                self.add_operation_to_t_queue(schedule_item)
                return False
            else:  # t isn't waiting
                self.final_schedule.append(schedule_item)
                logger.log_action(schedule_item)
        else: # is t still waiting
            if self.transaction_manager.is_t_waiting(schedule_item.transaction_id):
                self.add_operation_to_t_queue(schedule_item)
                return False
            else: # t isn't waiting but other transaction has the key
                if self.lock_table.is_other_t_has_X_key(schedule_item.transaction_id, schedule_item.resource):
                    self.add_operation_to_t_queue(schedule_item)
                    return False
                # t is free to act
                else:
                    self.final_schedule.append(self.lock_table.add_lock(schedule_item.transaction_id, schedule_item.resource, LockType.S))
                    self.final_schedule.append(schedule_item)
                    logger.log_action(schedule_item)

        return True

    def write(self, schedule_item: ScheduleItem, queue_action: bool = False) -> bool:
        # is t has X lock on resource
        if self.lock_table.is_t_has_X_lock(schedule_item.transaction_id, schedule_item.resource):
            # is t still waiting
            if self.transaction_manager.is_t_waiting(schedule_item.transaction_id):
                self.add_operation_to_t_queue(schedule_item)
                return False
            else:  # t isn't waiting
                self.final_schedule.append(schedule_item)
                logger.log_action(schedule_item)
        else:
            # is t still waiting
            if self.transaction_manager.is_t_waiting(schedule_item.transaction_id):
                self.add_operation_to_t_queue(schedule_item)
                return False
            else:
                # t isn't waiting but other transaction has key
                if self.lock_table.is_other_t_has_key(schedule_item.transaction_id, schedule_item.resource):
                    self.add_operation_to_t_queue(schedule_item)
                    return False
                # t is free to act
                else:
                    self.final_schedule.append(self.lock_table.add_lock(schedule_item.transaction_id, schedule_item.resource, LockType.X))
                    self.final_schedule.append(schedule_item)
                    logger.log_action(schedule_item)
        return True

    def commit(self, schedule_item: ScheduleItem, queue_action: bool = False) -> bool:
        # is t still waiting
        if self.transaction_manager.is_t_waiting(schedule_item.transaction_id):
            self.add_operation_to_t_queue(schedule_item)
        else:
            # t isn't waiting
            logger.log_action(schedule_item)

            # unlock all resource
            unlocked_resource, unlock_schedule_items = self.lock_table.unlock_transaction(schedule_item.transaction_id)
            self.final_schedule += unlock_schedule_items
            self.final_schedule.append(schedule_item)

            self.run_waiting_queue(unlocked_resource)
            return False

    def run_op(self, schedule_item: ScheduleItem, queue_action: bool = False) -> bool:
        if self.transaction_manager.is_t_abort(schedule_item.transaction_id):
            return False

        if schedule_item.action == Action.READ:
            return self.read(schedule_item, queue_action)
        elif schedule_item.action == Action.WRITE:
            return self.write(schedule_item, queue_action)
        else:
            return self.commit(schedule_item, queue_action)

    def run_waiting_queue(self, unlocked_resource: list[str]):
        # check is there any operation can be done
        for i, t in enumerate(self.transaction_manager.transactions):
            if not t.is_has_queue():
                continue
            do_next_op = False
            j = 0
            n_t_queue = len(t.queue)
            while j < n_t_queue:
                scheduleitem = t.queue[0]
                if do_next_op:
                    do_next_op = self.run_op(scheduleitem)
                    if do_next_op:
                        t.queue.pop(0)

                elif scheduleitem.resource in unlocked_resource:
                    t.status = TransactionStatus.RUNNING
                    do_next_op = self.run_op(scheduleitem)
                    t.queue.pop(0)

                if not do_next_op:
                    break
                j += 1

    def run(self):
        for schedule_item in self.schedule:
            # add to array of transaction
            if not self.transaction_manager.get_t_by_id(schedule_item.transaction_id):
                self.transaction_manager.add_transaction(schedule_item.transaction_id)

            # add schedule item to transaction
            t = self.transaction_manager.get_t_by_id(schedule_item.transaction_id)
            t.add_schedule(schedule_item)

            self.run_op(schedule_item)

        while self.transaction_manager.is_any_t_waiting():
            for t in self.transaction_manager.transactions:
                if t.is_waiting():
                    for schedule_item in t.queue:
                        self.run_op(schedule_item)

        for t in self.transaction_manager.transactions:
            if t.is_abort():
                t.status = TransactionStatus.RUNNING
                for schedule_item in t.schedules:
                    self.run_op(schedule_item)
