class QueueParams:
    passive: bool = False
    durable: bool = True
    auto_delete: bool = False
    no_ack: bool = True

    def __init__(self, passive=False, durable=True, auto_delete=False, no_ack=True):
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete
        self.no_ack = no_ack
