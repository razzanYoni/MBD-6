from utils import ScheduleItem


class Transaction:
    def __init__(self, t_id: str):
        self.id = t_id
        self.schedules: list[ScheduleItem] = []
        self.queue: list[ScheduleItem] = []
        self.abort = False

    def is_abort(self):
        return self.abort

    def is_waiting(self):
        return len(self.queue) != 0

    def add_schedule(self, schedule: ScheduleItem):
        self.schedules.append(schedule)


class TransactionManager:
    def __init__(self):
        self.transactions: list[Transaction] = []

    def get_t_by_id(self, t_id: str) -> Transaction | None:
        for t in self.transactions:
            if t.id == t_id:
                return t
        return None

    def add_transaction(self, t_id: str) -> None:
        self.transactions.append(Transaction(t_id))

    def is_t_older(self, t1: str, t2: str) -> bool:
        for t in self.transactions:
            if t.id == t1:
                return True
            if t.id == t2:
                return False
        return False

    def is_t_abort(self, t_id: str) -> bool:
        t = self.get_t_by_id(t_id)
        if t is None:
            return False
        return t.is_abort()

    def is_t_waiting(self, t_id: str) -> bool:
        t = self.get_t_by_id(t_id)
        if t is None:
            return False
        return t.is_waiting()
