from utils import log, log_symbol
from utils.schedule_parser import ScheduleItem
class Transaction:
  def __init__(self, id: str) -> None:
    self.id = id
    self.tstart = -1
    self.tvalidation = -1
    self.tfinish = -1
    self.is_started = False
    self.is_validated = False
    self.is_finished = False
    self.read_set: set[str] = set()
    self.write_set: set[str] = set()
    self.schedules: list[ScheduleItem] = []

  def reset(self):
    self.tstart = -1
    self.tvalidation = -1
    self.tfinish = -1
    self.is_started = False
    self.is_validated = False
    self.is_finished = False
    self.read_set.clear()
    self.write_set.clear()

  def start(self, tstart: int):
    log(self.id, log_symbol.INFO_SYMBOL, "starting")
    self.tstart = tstart
    self.is_started = True

  def validate(self, tvalidation: int):
    self.tvalidation = tvalidation
    self.is_validated = True

  def commit(self, tfinish: int):
    self.tfinish = tfinish
    self.is_finished = True

  def read(self, resource: str):
    self.read_set.add(resource)
  
  def write(self, resource: str):
    self.write_set.add(resource)