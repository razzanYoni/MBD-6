S = "read_locked"
X = "write_locked"
UNLOCKED = "unlocked"

def get_needed_locks(operation):
    if operation == "r":
        return S
    elif operation == "w":
        return X
    else:
        return None
    