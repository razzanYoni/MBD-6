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

  # def start(self, tstart: int):
  #   log(self.id, log_symbol.INFO_SYMBOL, "starting")
  #   self.tstart = tstart
  #   self.is_started = True

  def commit(self):
    self.is_committed = True

  def read(self, resource: str):
    self.read_set.add(resource)
  
  def write(self, resource: str):
    self.write_set.add(resource)