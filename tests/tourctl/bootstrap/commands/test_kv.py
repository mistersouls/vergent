# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for tourctl.bootstrap.commands.kv — put, get, delete CLI commands."""

from unittest.mock import AsyncMock, Mock, patch

from typer.testing import CliRunner

from tourctl.bootstrap.commands import kv
from tourctl.core.client import NodeUnreachableError, RequestTimeoutError, ServerError
from tourillon.bootstrap.handlers import KIND_KV_DELETE, KIND_KV_GET, KIND_KV_PUT
from tourillon.core.net.tcp.tls import TlsConfigurationError
from tourillon.core.structure.envelope import Envelope

runner = CliRunner()


def _make_serializer() -> Mock:
    s = Mock()
    s.encode = Mock(side_effect=lambda obj: b"encoded")
    s.decode = Mock(side_effect=lambda b: {})
    return s


def _make_client(response: Envelope | Exception) -> Mock:
    client = Mock()
    if isinstance(response, Exception):
        client.request = AsyncMock(side_effect=response)
    else:
        client.request = AsyncMock(return_value=response)
    return client


def test_put_payload_does_not_contain_now_ms() -> None:
    serializer = _make_serializer()
    # capture the dict passed to encode
    called: list[dict] = []

    def _encode(arg):
        called.append(arg)
        return b"p"

    serializer.encode = Mock(side_effect=_encode)
    serializer.decode = Mock(return_value={"wall": 1, "counter": 2, "node_id": "n"})

    client = _make_client(Envelope.create(b"r", kind=KIND_KV_PUT + ".ok"))

    with (
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "put",
                "--key",
                "k",
                "--value",
                "v",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    assert called, "serializer.encode was not called"
    payload = called[0]
    assert "now_ms" not in payload
    assert set(payload.keys()) == {"keyspace", "key", "value"}


def test_put_correct_kind_kv_put() -> None:
    serializer = _make_serializer()
    serializer.decode = Mock(return_value={"wall": 1, "counter": 2, "node_id": "n"})
    response = Envelope.create(b"r", kind=KIND_KV_PUT + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "put",
                "--key",
                "k",
                "--value",
                "v",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    # inspect the Envelope passed to client.request
    assert client.request.call_count == 1
    sent_envelope = client.request.call_args[0][0]
    assert sent_envelope.kind == KIND_KV_PUT


def test_put_renders_wall_counter_node_id_on_success() -> None:
    serializer = _make_serializer()
    serializer.decode = Mock(
        return_value={"wall": 42, "counter": 7, "node_id": "node-1"}
    )
    response = Envelope.create(b"r", kind=KIND_KV_PUT + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.print_key_value") as mock_print,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "put",
                "--key",
                "k",
                "--value",
                "v",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    mock_print.assert_called_once()
    rows = mock_print.call_args[0][1]
    assert ("wall", "42") in rows
    assert ("counter", "7") in rows
    assert ("node_id", "node-1") in rows


def test_get_payload_contains_keyspace_and_key() -> None:
    serializer = _make_serializer()

    captured: list[dict] = []

    def _encode(arg):
        captured.append(arg)
        return b"p"

    serializer.encode = Mock(side_effect=_encode)
    serializer.decode = Mock(return_value={"versions": []})

    response = Envelope.create(b"r", kind=KIND_KV_GET + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "get",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    assert captured, "encode not called"
    payload = captured[0]
    assert set(payload.keys()) == {"keyspace", "key"}


def test_get_renders_value_on_success() -> None:
    serializer = _make_serializer()
    serializer.decode = Mock(
        return_value={
            "versions": [{"value": b"hello", "wall": 1, "counter": 2, "node_id": "n"}]
        }
    )
    response = Envelope.create(b"r", kind=KIND_KV_GET + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.print_key_value") as mock_print,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "get",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    mock_print.assert_called_once()
    rows = mock_print.call_args[0][1]
    assert ("value", "hello") in rows


def test_get_prints_warning_when_versions_empty() -> None:
    serializer = _make_serializer()
    serializer.decode = Mock(return_value={"versions": []})
    response = Envelope.create(b"r", kind=KIND_KV_GET + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.print_warning") as mock_warn,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "get",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    mock_warn.assert_called_once()
    assert "not found" in mock_warn.call_args[0][0]


def test_delete_payload_does_not_contain_now_ms() -> None:
    serializer = _make_serializer()
    called: list[dict] = []

    def _encode(arg):
        called.append(arg)
        return b"p"

    serializer.encode = Mock(side_effect=_encode)
    serializer.decode = Mock(return_value={"wall": 1, "counter": 2, "node_id": "n"})

    response = Envelope.create(b"r", kind=KIND_KV_DELETE + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "delete",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    assert called, "encode not called"
    payload = called[0]
    assert "now_ms" not in payload
    assert set(payload.keys()) == {"keyspace", "key"}


def test_delete_correct_kind_kv_delete() -> None:
    serializer = _make_serializer()
    serializer.decode = Mock(return_value={"wall": 1, "counter": 2, "node_id": "n"})
    response = Envelope.create(b"r", kind=KIND_KV_DELETE + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "delete",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    assert client.request.call_count == 1
    sent_envelope = client.request.call_args[0][0]
    assert sent_envelope.kind == KIND_KV_DELETE


def test_delete_renders_success_message() -> None:
    serializer = _make_serializer()
    serializer.decode = Mock(return_value={"wall": 1, "counter": 2, "node_id": "n"})
    response = Envelope.create(b"r", kind=KIND_KV_DELETE + ".ok")
    client = _make_client(response)

    with (
        patch("tourctl.bootstrap.commands.kv.print_success") as mock_success,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "delete",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code == 0
    mock_success.assert_called_once()


def test_put_exits_on_node_unreachable_error() -> None:
    serializer = _make_serializer()
    client = _make_client(NodeUnreachableError("cannot reach"))

    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "put",
                "--key",
                "k",
                "--value",
                "v",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code != 0
    mock_error.assert_called_once()
    assert "Cannot reach node" in mock_error.call_args[0][0]


def test_put_exits_on_server_error() -> None:
    serializer = _make_serializer()
    client = _make_client(ServerError("boom"))

    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "put",
                "--key",
                "k",
                "--value",
                "v",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code != 0
    mock_error.assert_called_once()
    assert "boom" in mock_error.call_args[0][0]


def test_put_exits_on_tls_configuration_error() -> None:
    serializer = _make_serializer()
    # configure raises TLS error
    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        patch(
            "tourctl.bootstrap.commands.kv.deps.configure",
            new=Mock(side_effect=TlsConfigurationError("tls bad")),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "put",
                "--key",
                "k",
                "--value",
                "v",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code != 0
    mock_error.assert_called_once()


def test_get_exits_on_tls_configuration_error() -> None:
    serializer = _make_serializer()
    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        patch(
            "tourctl.bootstrap.commands.kv.deps.configure",
            new=Mock(side_effect=TlsConfigurationError("tls bad")),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "get",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code != 0
    mock_error.assert_called_once()


def test_delete_exits_on_tls_configuration_error() -> None:
    serializer = _make_serializer()
    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        patch(
            "tourctl.bootstrap.commands.kv.deps.configure",
            new=Mock(side_effect=TlsConfigurationError("tls bad")),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "delete",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code != 0
    mock_error.assert_called_once()


def test_handle_client_error_request_timeout() -> None:
    serializer = _make_serializer()
    client = _make_client(RequestTimeoutError(5.0))
    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "get",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
                "--timeout",
                "5",
            ],
        )

    assert result.exit_code != 0
    mock_error.assert_called_once()
    assert "timed out" in mock_error.call_args[0][0]


def test_handle_client_error_value_error() -> None:
    serializer = _make_serializer()
    client = _make_client(ValueError("bad"))
    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        patch("tourctl.bootstrap.commands.kv.deps.configure", new=Mock()),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer",
            new=Mock(return_value=serializer),
        ),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_client",
            new=Mock(return_value=client),
        ),
    ):
        result = runner.invoke(
            kv.app,
            [
                "get",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )

    assert result.exit_code != 0
    mock_error.assert_called_once()


def test_handle_client_error_unexpected_calls_print_error() -> None:
    """The default branch of _handle_client_error is only reachable when called directly."""
    import pytest

    from tourctl.bootstrap.commands.kv import _handle_client_error

    with (
        patch(
            "tourctl.bootstrap.commands.kv.print_error", side_effect=SystemExit(1)
        ) as mock_error,
        pytest.raises(SystemExit),
    ):
        _handle_client_error(RuntimeError("oops"), "localhost", 7000, 10.0)
    mock_error.assert_called_once()
    assert "oops" in str(mock_error.call_args[0][0])


def test_get_decodes_binary_value_as_hex_on_unicode_error() -> None:
    serializer = _make_serializer()
    # b"\xff\xfe" cannot be decoded as UTF-8
    serializer.decode = Mock(
        return_value={
            "versions": [
                {"value": b"\xff\xfe", "wall": 1, "counter": 1, "node_id": "n"}
            ]
        }
    )
    response = Envelope.create(b"r", kind=KIND_KV_GET + ".ok")
    client = _make_client(response)
    with (
        patch("tourctl.bootstrap.commands.kv.deps.configure"),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer", return_value=serializer
        ),
        patch("tourctl.bootstrap.commands.kv.deps.get_client", return_value=client),
        patch("tourctl.bootstrap.commands.kv.print_key_value") as mock_pkv,
    ):
        result = runner.invoke(
            kv.app,
            [
                "get",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )
    assert result.exit_code == 0
    mock_pkv.assert_called_once()
    rows = dict(mock_pkv.call_args[0][1])
    # hex representation of b"\xff\xfe" is "fffe"
    assert "fffe" in rows["value"]


def test_delete_exits_on_node_unreachable_error() -> None:
    serializer = _make_serializer()
    client = _make_client(NodeUnreachableError("cannot reach"))
    with (
        patch("tourctl.bootstrap.commands.kv.deps.configure"),
        patch(
            "tourctl.bootstrap.commands.kv.deps.get_serializer", return_value=serializer
        ),
        patch("tourctl.bootstrap.commands.kv.deps.get_client", return_value=client),
        patch(
            "tourctl.bootstrap.commands.kv.print_error",
            side_effect=SystemExit(1),
        ) as mock_err,
    ):
        result = runner.invoke(
            kv.app,
            [
                "delete",
                "--key",
                "k",
                "--certfile",
                "c",
                "--keyfile",
                "k",
                "--cafile",
                "a",
            ],
        )
    assert result.exit_code != 0
    mock_err.assert_called_once()
