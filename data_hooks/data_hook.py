from abc import ABC


class Datahook(ABC):
    def get_data(self):
        pass

    def get_raw_data(self):
        pass
