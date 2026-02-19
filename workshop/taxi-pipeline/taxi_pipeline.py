"""dlt pipeline to ingest NYC taxi data from the Zoomcamp REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

BASE_API_URL = (
    "https://us-central1-dlthub-analytics.cloudfunctions.net/"
    "data_engineering_zoomcamp_api"
)


@dlt.source
def taxi_pipeline_rest_api_source() -> dlt.sources.DltSource:
    """Define dlt resources from REST API endpoints using the REST API source."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": BASE_API_URL,
        },
        # "resource_defaults": {
        #     "primary_key": "key",
        #     "write_disposition": "replace",
        # },
        "resources": [
            {
                "name": "nyc_taxi_rides",
                "endpoint": {
                    "path": "",
                    "params": {},
                    "paginator": PageNumberPaginator(
                        base_page= 1,
                        page_param= "page",
                        total_path=False,
                        # Use a large upper bound and rely on
                        # `stop_after_empty_page` so we don't need a
                        # `total` field in the response.
                        maximum_page=100000,
                        stop_after_empty_page=True
                    )
                },
            },
        ],
    }

    yield from rest_api_resources(config)


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working incremental pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
