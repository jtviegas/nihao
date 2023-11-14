import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")))  # isort:skip


@pytest.fixture
def aws_credentials(monkeypatch):
    monkeypatch.setenv("S3_CONNECTOR_USE_CREDENTIALS", "1")
    monkeypatch.setenv("AWS_KEY_ID", "dummy")
    monkeypatch.setenv("AWS_SECRET_KEY", "dummy")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
