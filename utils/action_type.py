from enum import Enum


class Action(Enum):
  READ = "R"
  WRITE = "W"
  COMMIT = "C"
  ABORT = "A"
  WRITE_LOCAL = "WL"
  VALIDATE = "V"