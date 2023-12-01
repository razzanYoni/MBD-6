from utils import log, log_symbol
from utils.schedule_parser import ScheduleItem

class Transaction:
  def __init__(self, id: str, ts: int) -> None:
    self.id = id
    self.ts = ts
    self.schedules: list[ScheduleItem] = []
    self.is_committed = False

  def reset(self, ts: int):
    self.ts = ts
    self.is_committed = False

  def commit(self):
    self.is_committed = True