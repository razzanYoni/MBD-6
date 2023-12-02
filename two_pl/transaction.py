from utils import ScheduleItem


class Transaction:
    def __init__(self, id: str):
        self.id = id
        self.schedules: list[ScheduleItem] = []
        self.queue: list[ScheduleItem] = []
        self.abort = False

    def is_abort(self):
        return self.abort

    def is_has_queue(self):
        return len(self.queue) != 0