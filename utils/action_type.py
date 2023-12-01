from enum import Enum


class Action(Enum):
  READ = "r"
  WRITE = "w"
  COMMIT = "c"
  ABORT = "a"
  WRITE_LOCAL = "wl"
  VALIDATE = "v"