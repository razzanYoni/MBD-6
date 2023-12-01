from . import log_symbol

from .action_type import Action
from .schedule_parser import ScheduleItem

action_str_map = {
    Action.COMMIT: 'committing',
    Action.READ: 'reading',
    Action.WRITE: 'writing',
    Action.VALIDATE: 'validating'
}

def log(transaction_id, symbol, description):
        print(f"{symbol} [T{transaction_id}] {description}")

def log_action(operation: ScheduleItem):
    log(operation.transaction_id, log_symbol.ACTION_SYMBOL, f"{action_str_map[operation.action]} {operation.resource if operation.resource else ''}")

def log_abort(transaction_id: str):
    log(transaction_id, log_symbol.)
    pass