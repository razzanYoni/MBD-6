from enum import Enum

class Action(Enum):
  READ = "r"
  WRITE = "w"
  COMMIT = "c"