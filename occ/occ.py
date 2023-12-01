from utils import Action, log_symbol
from utils.logger import log, log_abort, log_action
from utils.schedule_parser import ScheduleItem

from .transaction import Transaction


class OCC:
  def __init__(self, schedule: list[ScheduleItem]) -> None:
    self.schedule = schedule
    self.out: list[ScheduleItem] = []
    self.transactions: dict[str, Transaction] = {}
    self._init_transactions()

  def _init_transactions(self) -> None:
    for item in self.schedule:
      transaction_id = item.transaction_id
      if transaction_id not in self.transactions:
        self.transactions[transaction_id] = Transaction(transaction_id)
      self.transactions[transaction_id].schedules.append(item)

  def run(self):
    i = 0
    while i < len(self.schedule):
      item = self.schedule[i]
      transaction = self.transactions[item.transaction_id]
      if not transaction.is_started:
        transaction.start(i)

      if transaction.is_finished:
        raise Exception(f"Invalid schedule, transaction {transaction.id} has been finished")
      
      if item.action == Action.READ:
        self.out.append(item)
        log_action(item)
        transaction.read(item.resource)
      elif item.action == Action.WRITE:
        self.out.append(ScheduleItem(Action.WRITE_LOCAL, item.transaction_id, item.resource))
        log(item.transaction_id, log_symbol.INFO_SYMBOL, f"writing local {item.resource}")
        transaction.write(item.resource)
      else:
        self.out.append(ScheduleItem(Action.VALIDATE, item.transaction_id))
        log(transaction.id, log_symbol.INFO_SYMBOL, "validating")
        is_valid = True
        for t_other in self.transactions.values():
          if t_other.id == transaction.id or not t_other.is_started or not t_other.is_finished:
            continue
          # finishTS(Ti) < startTS(Tj)
          if t_other.tfinish < transaction.tstart:
            continue

          # in this case, startTS(Tj) < finishTS(Ti) < validationTS(Tj) is True
          # check set of data
          if len(transaction.read_set.intersection(t_other.write_set)) == 0:
            continue
          # abort
          is_valid = False
          log_abort(transaction.id)
          j = len(self.schedule) - 1
          while j > i:
            if self.schedule[j].transaction_id == transaction.id:
              self.schedule.pop(j)
            j -= 1
          transaction.reset()
          self.out.append(ScheduleItem(Action.ABORT, transaction.id))
          self.schedule.extend(transaction.schedules)
          break

        if is_valid:
          for resource in transaction.write_set:
            self.out.append(ScheduleItem(Action.WRITE, transaction.id, resource))
            log(transaction.id, log_symbol.INFO_SYMBOL, f"writing {resource}")
          self.out.append(ScheduleItem(Action.COMMIT, transaction.id))
          log_action(item)
          transaction.commit(i)

      i += 1
      