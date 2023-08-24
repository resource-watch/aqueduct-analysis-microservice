import os
import pytest
from moto import mock_logs


@pytest.fixture(scope="package")
def client():
    mocked_log = mock_logs()
    mocked_log.start()

    from aqueduct import app

    if not os.getenv("GATEWAY_URL"):
        raise Exception("GATEWAY_URL needs to be set")
    if not os.getenv("MICROSERVICE_TOKEN"):
        raise Exception("MICROSERVICE_TOKEN needs to be set")

    # app = aqueduct.app
    app.config["TESTING"] = True
    client = app.test_client()

    yield client
    mocked_log.stop()
