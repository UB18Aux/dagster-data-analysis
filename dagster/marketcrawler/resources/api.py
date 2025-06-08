import dagster as dg
import requests

from requests import Response


class ApiEndpointResource(dg.ConfigurableResource):
    api_endpoint: str

    def request(self, item_id: str, time: str) -> Response:
        return requests.get(
            f"{self.api_endpoint}/price",
            params={
                "item_id": item_id,
                "time": time,
            },
        )


Api = ApiEndpointResource(api_endpoint="http://price-api:8000")
