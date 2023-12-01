from utils.action_type import Action

def parse_data(file):
    arr: list[ScheduleItem] = []
    with open (file, "r") as f:
        for line in f:
            line = line.strip()
            if line[0] == "c":
                action, transaction_id = line.split(",")
                arr.append(ScheduleItem(Action.COMMIT, transaction_id))
            else :
                action, transaction_id, resource = line.split(",")
                if action == "r":
                    arr.append(ScheduleItem(Action.READ, transaction_id, resource))
                elif action == "w":
                    arr.append(ScheduleItem(Action.WRITE, transaction_id, resource))
                else:
                    raise Exception("Invalid action")
    
    return arr

class ScheduleItem:
    def __init__(self, action: Action, transaction_id: str, resource: str = None) -> None:
        self.action = action
        self.transaction_id = transaction_id
        self.resource = resource
