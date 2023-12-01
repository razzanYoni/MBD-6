from utils.action_type import Action

S = "S"
X = "X"
UNLOCKED = "unlocked"

def get_needed_locks(operation):
    if operation == Action.READ:
        return "S"
    elif operation == Action.WRITE:
        return "X"
    else:
        return None