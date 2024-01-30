class ExchangeParams:
    passive: bool = False
    durable: bool = True
    auto_delete: bool = False

    def __init__(self, passive=False, durable=True, auto_delete=False):
        self.passive = passive
        self.durable = durable
        self.auto_delete = auto_delete
