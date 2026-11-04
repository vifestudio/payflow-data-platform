import sys

from airbyte_cdk.entrypoint import AirbyteEntrypoint
from source import SourceFxRates

if __name__ == "__main__":
    source = SourceFxRates()
    entrypoint = AirbyteEntrypoint(source)
    parsed_args = AirbyteEntrypoint.parse_args(sys.argv[1:])
    messages = entrypoint.run(parsed_args)
    for message in messages:
        print(message)
