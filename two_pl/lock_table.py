from enum import Enum
from typing import Dict

import utils.log_symbol as log_symbol
import utils.logger as logger
from utils.schedule_parser import ScheduleItem
from utils.action_type import Action


class LockType(Enum):
    X = "EXCLUSIVE"
    S = "SHARED"


class LockItem:
    def __init__(self, t_id: str, resource: str, lock_type: LockType):
        self.t_id: str = t_id
        self.resource: str = resource
        self.lock_type: LockType = lock_type

    def upgrade(self):
        self.lock_type: LockType = LockType.X


class LockTable:
    def __init__(self):
        self.lock_per_resource: Dict[str, list[LockItem]] = {}

    def add_lock(self, t_id: str, resource: str, lock_type: LockType):
        lock_item = LockItem(t_id, resource, lock_type)
        if resource not in self.lock_per_resource:
            self.lock_per_resource[resource]: list[LockItem] = [lock_item]
        else:
            self.lock_per_resource[resource].append(lock_item)

        logger.log(t_id, log_symbol.INFO_SYMBOL, f"Added lock: {lock_type} on {resource}")
        return ScheduleItem(Action.SHARED if lock_type == LockType.S else Action.EXCLUSIVE,
                            t_id,
                            resource)

    def upgrade_lock(self, t_id: str, resource: str):
        for lock_item in self.lock_per_resource[resource]:
            if lock_item.t_id == t_id:
                lock_item.upgrade()
                break

        logger.log(t_id, log_symbol.INFO_SYMBOL, f"Upgrade lock on {resource}")
        return ScheduleItem(Action.UPGRADE_LOCK, t_id, resource)

    def unlock(self, t_id: str, resource: str):
        for lock_item in self.lock_per_resource[resource]:
            if lock_item.t_id == t_id:
                self.lock_per_resource[resource].remove(lock_item)
                break

        logger.log(t_id, log_symbol.INFO_SYMBOL, f"Unlocked {resource}")
        return ScheduleItem(Action.UNLOCK, t_id, resource)

    def unlock_transaction(self, t_id: str):
        logger.log(t_id, log_symbol.INFO_SYMBOL, f"Unlocked all resources")
        resources = []
        unlock_schedule_items = []
        for resource in self.lock_per_resource:
            for lock_item in self.lock_per_resource[resource]:
                if lock_item.t_id == t_id:
                    unlock_schedule_items.append(self.unlock(lock_item.t_id, lock_item.resource))
                    resources.append(lock_item.resource)
                    break

        return resources, unlock_schedule_items

    def is_resource_unlocked(self, resource: str):
        return resource not in self.lock_per_resource or len(self.lock_per_resource[resource]) == 0

    def get_t_lock_on_resource(self, t_id: str, resource: str):
        if self.is_resource_unlocked(resource): return None
        for lock_item in self.lock_per_resource[resource]:
            if lock_item.t_id == t_id:
                return lock_item.lock_type
        return None

    def is_t_has_lock(self, t_id: str, resource: str):
        return self.get_t_lock_on_resource(t_id, resource) is not None

    def is_t_has_S_lock(self, t_id: str, resource: str):
        return self.get_t_lock_on_resource(t_id, resource) == LockType.S

    def is_t_has_X_lock(self, t_id: str, resource: str):
        return self.get_t_lock_on_resource(t_id, resource) == LockType.X

    def other_t_key_on_resource(self, t_id: str, resource: str):
        if self.is_resource_unlocked(resource): return None

        for lock_item in self.lock_per_resource[resource]:
            if lock_item.t_id != t_id:
                return lock_item.lock_type
        return None

    def is_other_t_has_key(self, t_id: str, resource: str):
        return self.other_t_key_on_resource(t_id, resource) is not None

    def is_other_t_has_X_key(self, t_id: str, resource: str):
        return self.other_t_key_on_resource(t_id, resource) == LockType.X

    def get_lock_item_by_resource(self, resource: str):
        if self.is_resource_unlocked(resource): return []
        return self.lock_per_resource[resource]
