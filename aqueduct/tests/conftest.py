import os
import pytest
from moto import mock_logs

# Activate moto's CloudWatch Logs mock at conftest import time, BEFORE any
# test module imports `aqueduct`. Importing `aqueduct/__init__.py` triggers
# RWAPIMicroservicePython.register(), which (when CloudWatch logging is
# enabled) instantiates a boto3 logs client and calls create_log_group().
# Without the mock active at that moment, the call hits real AWS — and on
# Jenkins/EKS the assumed role has no logs:CreateLogGroup permission, so
# the import (and pytest collection) fails with AccessDeniedException.
#
# AWS_CLOUD_WATCH_LOGGING_ENABLED=False in docker-compose-test.yml is the
# primary defense; this mock is the safety net. Dummy AWS credentials are
# required for moto to intercept — boto3 raises NoCredentialsError at the
# signing stage before any moto handler runs if creds are absent.
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
_mocked_logs = mock_logs()
_mocked_logs.start()

# Shim: Flask 2.2.x reads `werkzeug.__version__` but Werkzeug 3.x dropped
# that attribute. Restore it via importlib.metadata so `app.test_client()`
# can build the test environ. No-op on older Werkzeug that already has it.
import werkzeug

if not hasattr(werkzeug, "__version__"):
    import importlib.metadata as _md

    werkzeug.__version__ = _md.version("werkzeug")


@pytest.fixture(scope="package")
def client():
    from aqueduct import app

    if not os.getenv("GATEWAY_URL"):
        raise Exception("GATEWAY_URL needs to be set")
    if not os.getenv("MICROSERVICE_TOKEN"):
        raise Exception("MICROSERVICE_TOKEN needs to be set")

    app.config["TESTING"] = True
    client = app.test_client()

    yield client
