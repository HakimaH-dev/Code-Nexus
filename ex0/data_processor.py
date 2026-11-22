import typing

from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self):
        self.data = []
        self.count = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        try:
            valeur = self.data.pop(0)
            tu = (self.count, valeur)
            self.count += 1
            return tu
        except IndexError as e:
            print(f"{e}")


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list) and all(
            isinstance(item, (int, float))for item in data
        ):
            return True
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("improper numeric data")

        else:
            if isinstance(data, list):
                for item in data:
                    self.data.append(str(item))
            else:
                self.data.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list) and all(
            isinstance(item, str)for item in data
        ):
            return True
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError()
        else:
            if isinstance(data, list):
                for item in data:
                    self.data.append(item)
            else:
                self.data.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data) -> bool:
        if isinstance(data, dict) and all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in data.items()
        ):
            return True

        elif isinstance(data, list) and all(
            isinstance(dictionnaire, dict) and all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in dictionnaire.items()
            )
            for dictionnaire in data
        ):
            return True
        else:
            return False

    def ingest(
        self, data: dict[str, str] | list[dict[str, str]]
    ) -> None:
        if not self.validate(data):
            raise ValueError()
        else:
            if isinstance(data, list):
                for item in data:
                    self.data.append(str(item))
            else:
                self.data.append(str(data))


fauxnum = NumericProcessor()
vrainum = NumericProcessor()
num2 = NumericProcessor()
num = NumericProcessor()
vide = NumericProcessor()

textvrais = TextProcessor()
textfaux = TextProcessor()

log_false = LogProcessor()
log_true = LogProcessor()


def data_processor():

    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    result = vrainum.validate(42)
    print(f"trying to validate input '42' : {result}")
    resultat = fauxnum.validate("Hello")
    print(f"trying to validate input {fauxnum.data}: {resultat}")
    try:
        fauxnum.ingest("foo")
    except ValueError as e:
        print(
            "Test invalid ingestion of string 'foo' without prior validation:"
        )
        print(f"Got exception: {e}")
    num.ingest([1, 2, 3, 4, 5])
    print(f"processing data: {num.data}")
    print("Extracting 3 values...")
    donne = num.output()
    print(f"Numeric values {donne[0]}: {donne[1]}")
    donne = num.output()
    print(f"Numeric values {donne[0]}: {donne[1]}")
    donne = num.output()
    print(f"Numeric values {donne[0]}: {donne[1]} \n")
    # num2.ingest([])
    # print(f"liste vide:{num2.data}")
    # num2.ingest(43)
    # print(f"entier seul:{num2.data}")
    # vide.output()
    print("Testing Text Processor...")
    teste = textfaux.validate(42)
    print(f"Trying to validate input '42':{teste} ")
    textvrais.ingest(["Hello", "Nexus", "World"])
    print(f"Pocessing data: {textvrais.data}")
    donne_text = textvrais.output()
    print("Extracting 1 value...")
    print(f"Text value {donne_text[0]}: {donne_text[1]}\n")

    print("Testing Log Processor...")
    lo = log_false.validate("Hello")
    print(f"Trying to validate input 'Hello': {lo}")

    # v = log_true.validate([{"fvfvre":"dcdc"} ,{"ddd":"ss"}])
    # print(f"Trying to validate input log level: {v}")
    log_true.ingest([
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"}])
    print(f"Processing data: {log_true.data}")
    donne_log = log_true.output()
    print("Extracting 2 values...")
    print(f"Log entry {donne_log[0]}: {donne_log[1]}")
    donne_log = log_true.output()
    print(f"Log entry {donne_log[0]}: {donne_log[1]}")


if __name__ == "__main__":
    data_processor()
