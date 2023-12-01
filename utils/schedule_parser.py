from typing import override

from utils.action_type import Action


class ScheduleParser:
    def __init__(self, file: str) -> None:
        self.file = file
        self.schedule: list[ScheduleItem] = []
        with open(file, "r") as f:
            for line in f:
                line = line.strip()
                self.schedule.append(self._parse_row(line))

    def _parse_row(self, line: str):
        if line[0] == "c":
            action, transaction_id = line.split(",")
            return ScheduleItem(Action.COMMIT, transaction_id)
        else :
            action, transaction_id, resource = line.split(",")
            if action == "r":
                return ScheduleItem(Action.READ, transaction_id, resource)
            elif action == "w":
                return ScheduleItem(Action.WRITE, transaction_id, resource)
            else:
                raise Exception("Invalid action")

class ScheduleParserValidate(ScheduleParser):
    def __init__(self, file: str) -> None:
        super().__init__(file)

    @override
    def _parse_row(self, line):
        if line[0] == "v":
            _, transaction_id = line.split(",")
            return ScheduleItem(Action.VALIDATE, transaction_id)
        return super()._parse_row(line)

class ScheduleItem:
    def __init__(self, action: Action, transaction_id: str, resource: str = None) -> None:
        self.action = action
        self.transaction_id = transaction_id
        self.resource = resource
