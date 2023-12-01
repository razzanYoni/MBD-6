class SingletonMeta(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class Logger(metaclass=SingletonMeta):
    def __init__(self):
        self.queue = []
        self.rollback_queue = []

    def log(self, transaction, symbol, description):
        self.queue.append([transaction, symbol, description])

    def rollback_log(self, transaction) :
        for log in self.queue:
            if log[0] == transaction:
                self.rollback_queue.append(log)
                self.queue.remove(log)

    def __str__(self) -> str:
        for tx, symbol, description in self.queue:
            print(description)