from utils.schedule_parser import ScheduleItem
from .transaction import Transaction
from utils import Logger


class OCC:
  def __init__(self, schedule: list[ScheduleItem]) -> None:
    self.schedule = schedule
    self.transactions: dict[str, Transaction] = {}
    self.lock_table = {}
    self.logger = Logger()
    self._init_transactions()
    self._init_lock_table()

  def _init_transactions(self) -> None:
    for item in self.schedule:
      transaction_id = item.transaction_id
      if transaction_id not in self.transactions:
        self.transactions[transaction_id] = Transaction(transaction_id)