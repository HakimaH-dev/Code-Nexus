import typing

from abc import ABC, abstractmethod


class ExportPlugin(typing.Protocol):
    def process_output(
        self, data: list[tuple[int, str]]
    ) -> None:
        pass


class JSONPlugin:
    def process_output(
        self, data: list[tuple[int, str]]
    ) -> None:
        print("JSON Output:")

        resultat = []
        for index, valeur in data:
            resultat.append(
                f'"item_{index}": "{valeur}"'
            )

        print("{" + ", ".join(resultat) + "}")


class CSVPlugin:
    def process_output(
        self, data: list[tuple[int, str]]
    ) -> None:
        print("CSV Output:")

        resultat = []
        for index, valeur in data:
            resultat.append(valeur)

        print(",".join(resultat))


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
                    valeur = (
                        item["log_level"] + ": " + item["log_message"]
                    )
                    self.data.append(valeur)
                    self.count += 1
            else:
                valeur = (
                    data["log_level"] + ": " + data["log_message"]
                )
                self.data.append(valeur)
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

    def output_pipeline(
        self, nb: int, plugin: ExportPlugin
    ) -> None:
        for proc in self.processor:
            resultat = []

            for i in range(min(nb, len(proc.data))):
                valeur = proc.output()
                resultat.append(valeur)

            if resultat:
                plugin.process_output(resultat)

    def print_processor_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self.processor:
            print("No processor found, no data")
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

csv_plugin = CSVPlugin()
json_plugin = JSONPlugin()


def data_pipeline():
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    datastream.print_processor_stats()

    print("\nRegistering Processors")

    datastream.register_processor(numeric)
    datastream.register_processor(txt)
    datastream.register_processor(log)

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
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]

    print("\nSend first batch of data on stream:", stream)
    datastream.process_stream(stream)
    print()
    datastream.print_processor_stats()

    print("\nSend 3 processed data from each processor "
          "to a CSV plugin:")
    datastream.output_pipeline(3, csv_plugin)

    print()
    datastream.print_processor_stats()

    stream = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {
                "log_level": "ERROR",
                "log_message": "500 server crash"
            },
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days"
            }
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print("\nSend another batch of data:", stream)
    datastream.process_stream(stream)

    print()
    datastream.print_processor_stats()

    print("\nSend 5 processed data from each processor "
          "to a JSON plugin:")
    datastream.output_pipeline(5, json_plugin)

    print()
    datastream.print_processor_stats()


if __name__ == "__main__":
    data_pipeline()
