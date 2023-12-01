from utils import Action, Logger
from utils.schedule_parser import ScheduleItem

from .transaction import Transaction


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

  def run(self):
    for i, item in enumerate(self.schedule):
      transaction = self.transactions[item.transaction_id]
      if transaction.tstart == -1:
        transaction.tstart = i
      if transaction.tfinish != -1:
        raise Exception(f"Invalid schedule, transaction {transaction.id} has been finished")

      if item.action == Action.COMMIT:
        transaction.tfinish = i
      
      if item.action == Action.READ:
        transaction.read(item.resource)
      elif item.action == Action.WRITE:
        transaction.write(item.resource)