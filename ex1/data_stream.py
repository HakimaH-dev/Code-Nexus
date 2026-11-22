import typing

from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self):
        self.data = []
        self.count = 0
        self.output_count = 0

    @abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        valeur = self.data.pop(0)
        tu = (self.output_count, valeur)
        self.output_count += 1
        return tu


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list) and all(
            isinstance(item, (int, float)) for item in data
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
                    self.count += 1
            else:
                self.data.append(str(data))
                self.count += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list) and all(
            isinstance(item, str) for item in data
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
                    self.count += 1
            else:
                self.data.append(data)
                self.count += 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
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
                    self.count += 1
            else:
                self.data.append(str(data))
                self.count += 1


class DataStream:
    def __init__(self):
        self.processor = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processor.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            pro = False

            for proc in self.processor:
                if proc.validate(item):
                    proc.ingest(item)
                    pro = True
                    break

            if not pro:
                print(
                    f"DataStream error - Can't process element "
                    f"in stream: {item}"
                )

    def print_processor_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self.processor:
            print("No processor found, no data\n")
            return

        for proc in self.processor:
            if isinstance(proc, NumericProcessor):
                name = "Numeric Processor"
            elif isinstance(proc, TextProcessor):
                name = "Text Processor"
            elif isinstance(proc, LogProcessor):
                name = "Log Processor"

            print(
                f"{name}: total {proc.count} items processed, "
                f"remaining {len(proc.data)} on processor"
            )


datastream = DataStream()
numeric = NumericProcessor()
txt = TextProcessor()
log = LogProcessor()


def data_stream():
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    datastream.print_processor_stats()

    print("Registering Numeric Processor\n")
    datastream.register_processor(numeric)

    stream = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]

    print("Send first batch of data on stream:", stream)
    datastream.process_stream(stream)
    datastream.print_processor_stats()

    print("Registering other data processors")
    datastream.register_processor(txt)
    datastream.register_processor(log)

    print("Send the same batch again")
    datastream.process_stream(stream)
    datastream.print_processor_stats()

    print("Consume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")

    for i in range(3):
        numeric.output()

    for i in range(2):
        txt.output()

    log.output()

    datastream.print_processor_stats()


if __name__ == "__main__":
    data_stream()
