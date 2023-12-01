from action_type import READ, WRITE, COMMIT

def parse_data(file):
    arr = []
    with open (file, "r") as f:
        for line in f:
            line = line.strip()
            if line[0] == "c":
                action, transaction_id = line.split(",")
                arr.append([COMMIT, transaction_id])
            else :
                action, transaction_id, resource = line.split(",")
                if action == "r":
                    arr.append([READ, transaction_id, resource])
                elif action == "w":
                    arr.append([WRITE, transaction_id, resource])
                else:
                    raise Exception("Invalid action")
    
    return arr
