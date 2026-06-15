import os
import pytest
from moto import mock_logs

# Shim: Flask 2.2.x reads `werkzeug.__version__` but Werkzeug 3.x dropped
# that attribute. Restore it via importlib.metadata so `app.test_client()`
# can build the test environ. No-op on older Werkzeug that already has it.
import werkzeug

if not hasattr(werkzeug, "__version__"):
    import importlib.metadata as _md

    werkzeug.__version__ = _md.version("werkzeug")


@pytest.fixture(scope="package")
def client():
    mocked_log = mock_logs()
    mocked_log.start()

    from aqueduct import app

    if not os.getenv("GATEWAY_URL"):
        raise Exception("GATEWAY_URL needs to be set")
    if not os.getenv("MICROSERVICE_TOKEN"):
        raise Exception("MICROSERVICE_TOKEN needs to be set")

    app.config["TESTING"] = True
    client = app.test_client()

    yield client
    mocked_log.stop()
