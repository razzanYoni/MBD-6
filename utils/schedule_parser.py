def parse_data(file):
    arr = []
    with open (file, "r") as f:
        for line in f:
            line = line.strip()
            if line[0] == "c":
                action, transaction_id = line.split(",")
                arr.append([action, transaction_id])
            else :
                action, transaction_id, resource = line.split(",")
                if action == "b":
                    arr.append([action, transaction_id, resource])
                elif action == "r":
                    arr.append([action, transaction_id, resource])
                elif action == "w":
                    arr.append([action, transaction_id, resource])
                else:
                    raise Exception("Invalid action")
    
    return arr
