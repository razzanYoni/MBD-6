from .data import Data
from utils import Action, log_symbol
from utils.logger import log, log_abort, log_action
from utils.schedule_parser import ScheduleItem

from .transaction import Transaction


class MVCC:
  def __init__(self, schedule: list[ScheduleItem]) -> None:
    self.schedule = schedule
    self.transactions: dict[str, Transaction] = {}
    self.database: dict[str, Data] = {}
    self.lock_table = {}
    self.ts = 1
    self._init_transactions()

  def _init_transactions(self) -> None:
    for item in self.schedule:
      transaction_id = item.transaction_id
      if transaction_id not in self.transactions:
        self.transactions[transaction_id] = Transaction(transaction_id, self.ts)
        self.ts += 1
      self.transactions[transaction_id].schedules.append(item)
      if item.resource is not None and item.resource not in self.database:
          self.database[item.resource] = Data(item.resource)

  def run(self):
    i = 0
    while i < len(self.schedule):
      item = self.schedule[i]
      transaction = self.transactions[item.transaction_id]
      if transaction.is_committed:
        raise Exception(f"Invalid schedule, transaction {transaction.id} has been committed")

      if item.action == Action.READ or item.action == Action.WRITE:
        data = self.database[item.resource]
        version_read = None
        # get max version that write timestamp is less than or equal to transaction ts
        for j in range(len(data.versions)):
          if data.versions[j][1] <= transaction.ts:
            if version_read is None or data.versions[j][1] > data.versions[version_read][1]:
              version_read = j
            
        version = data.versions[version_read]
        if item.action == Action.READ:
          is_update = transaction.ts > version[0]
          if is_update:
            version[0] = transaction.ts
          log(transaction.id, log_symbol.INFO_SYMBOL, f"reading {item.resource}{version_read}. TS({item.resource}{version_read}): {version}")
        else:
          if transaction.ts < version[0]:
            # rollback
            log_abort(transaction.id)
            j = len(self.schedule) - 1
            while j > i:
              if self.schedule[j].transaction_id == transaction.id:
                self.schedule.pop(j)
              j -= 1
            transaction.reset(self.ts)
            log(transaction.id, log_symbol.INFO_SYMBOL, f"new timestamp: {transaction.ts}")
            self.ts += 1
            self.schedule.extend(transaction.schedules)
            self.schedule[i] = ScheduleItem(Action.ABORT, transaction.id)
          elif transaction.ts == version[1]:
            # overwrite
            log(transaction.id, log_symbol.INFO_SYMBOL, f"overwriting {item.resource}{version_read}. TS({item.resource}{version_read}): {version}")
          else:
            # insert
            log(transaction.id, log_symbol.INFO_SYMBOL, f"create new version {item.resource}{len(data.versions)}. TS({item.resource}{len(data.versions)}): [{transaction.ts}, {transaction.ts}]")
            data.versions.append([transaction.ts, transaction.ts])
      else:
        # commit
        log_action(item)
        transaction.commit()
      i += 1

    print("Final data timestamp and version:")
    for resource in self.database:
      data = self.database[resource]
      print(f"{resource}: {data.versions}")
