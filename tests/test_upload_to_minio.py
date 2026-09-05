import pytest
from botocore.exceptions import ClientError

import upload_to_minio as mod


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": "boom"}}, "Operation")


class FakeS3Client:
    def __init__(self, head_object_error=None, head_bucket_error=None):
        self.head_object_error = head_object_error
        self.head_bucket_error = head_bucket_error
        self.created_buckets = []

    def head_object(self, Bucket, Key):
        if self.head_object_error:
            raise self.head_object_error
        return {}

    def head_bucket(self, Bucket):
        if self.head_bucket_error:
            raise self.head_bucket_error
        return {}

    def create_bucket(self, Bucket):
        self.created_buckets.append(Bucket)


def test_require_env_returns_value(monkeypatch):
    monkeypatch.setenv("SOME_VAR", "value")
    assert mod._require_env("SOME_VAR") == "value"


def test_require_env_raises_when_missing(monkeypatch):
    monkeypatch.delenv("SOME_VAR", raising=False)
    with pytest.raises(RuntimeError, match="SOME_VAR"):
        mod._require_env("SOME_VAR")


def test_require_env_raises_when_empty(monkeypatch):
    monkeypatch.setenv("SOME_VAR", "")
    with pytest.raises(RuntimeError, match="SOME_VAR"):
        mod._require_env("SOME_VAR")


def test_object_exists_true():
    assert mod.object_exists(FakeS3Client(), "bucket", "key") is True


def test_object_exists_false_on_404():
    client = FakeS3Client(head_object_error=_client_error("404"))
    assert mod.object_exists(client, "bucket", "key") is False


def test_object_exists_false_on_nosuchkey():
    client = FakeS3Client(head_object_error=_client_error("NoSuchKey"))
    assert mod.object_exists(client, "bucket", "key") is False


def test_object_exists_reraises_other_errors():
    client = FakeS3Client(head_object_error=_client_error("AccessDenied"))
    with pytest.raises(ClientError):
        mod.object_exists(client, "bucket", "key")


def test_ensure_bucket_noop_when_exists():
    client = FakeS3Client()
    mod.ensure_bucket(client, "bucket")
    assert client.created_buckets == []


def test_ensure_bucket_creates_when_missing():
    client = FakeS3Client(head_bucket_error=_client_error("404"))
    mod.ensure_bucket(client, "bucket")
    assert client.created_buckets == ["bucket"]


def test_ensure_bucket_reraises_other_errors():
    client = FakeS3Client(head_bucket_error=_client_error("AccessDenied"))
    with pytest.raises(ClientError):
        mod.ensure_bucket(client, "bucket")


def test_get_client_raises_without_credentials(monkeypatch):
    monkeypatch.delenv("MINIO_ROOT_USER", raising=False)
    monkeypatch.delenv("MINIO_ROOT_PASSWORD", raising=False)
    with pytest.raises(RuntimeError, match="MINIO_ROOT_USER"):
        mod.get_client()


def test_get_client_builds_boto3_client_with_credentials(monkeypatch):
    monkeypatch.setenv("MINIO_ROOT_USER", "admin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "secret")
    captured = {}

    def fake_client(service_name, **kwargs):
        captured["service_name"] = service_name
        captured.update(kwargs)
        return "fake-client"

    monkeypatch.setattr(mod.boto3, "client", fake_client)
    result = mod.get_client()

    assert result == "fake-client"
    assert captured["service_name"] == "s3"
    assert captured["aws_access_key_id"] == "admin"
    assert captured["aws_secret_access_key"] == "secret"
