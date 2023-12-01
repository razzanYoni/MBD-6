from . import log_symbol

from .action_type import Action
from .schedule_parser import ScheduleItem

action_str_map = {
    Action.COMMIT: 'committing',
    Action.READ: 'reading',
    Action.WRITE: 'writing',
}

def log(transaction_id: str, symbol: str, description: str):
        print(f"{symbol} [T{transaction_id}] {description}")

def log_action(operation: ScheduleItem):
    log(operation.transaction_id, log_symbol.INFO_SYMBOL, f"{action_str_map[operation.action]} {operation.resource if operation.resource else ''}")

def log_add_to_queue(operation):
    log(operation.transaction_id, log_symbol.INFO_SYMBOL, f"{action_str_map[operation.action]} {operation.resource if operation.resource else ''} adding to queue")

def log_abort(transaction_id: str):
    log(transaction_id, log_symbol.CRITICAL_SYMBOL, "aborting")