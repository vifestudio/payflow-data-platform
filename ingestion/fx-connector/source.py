import requests
from typing import Any, Iterable, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams.http import HttpStream


class FxRates(HttpStream):
    url_base = "http://host.docker.internal:8001/"
    primary_key = ["date", "currency"]

    def path(self, **kwargs) -> str:
        return "rates"

    def next_page_token(self, response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response, **kwargs) -> Iterable[Mapping]:
        yield from response.json()

    def request_params(self, stream_state=None, stream_slice=None, next_page_token=None):
        return {}


class SourceFxRates(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            resp = requests.get("http://host.docker.internal:8001/health", timeout=5)
            if resp.status_code == 200:
                return True, None
            return False, f"Health check returned {resp.status_code}"
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List:
        return [FxRates()]
