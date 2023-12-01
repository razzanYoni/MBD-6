from utils import log, log_symbol
class Transaction:
  def __init__(self, id: str) -> None:
    self.id = id
    self.phase = 0
    self.tstart = -1
    self.tvalidation = -1
    self.tfinish = -1
    self.read_set: set[str] = set()
    self.write_set: set[str] = set()

  def read(self, resource: str):
    log(self.id, log_symbol.INFO_SYMBOL, )
    self.read_set.add(resource)
  
  def write(self, resource: str):
    self.write_set.add(resource)