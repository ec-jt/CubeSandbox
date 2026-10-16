"""Dynamic port contract tests, all HTTP mocked."""
from unittest.mock import Mock

import pytest
import requests

from cubesandbox import Sandbox
from cubesandbox._config import Config
from cubesandbox._exceptions import ApiError, AuthenticationError, SandboxNotFoundError


@pytest.fixture
def sandbox():
    sb = Sandbox({"sandboxID": "sb-1", "templateID": "tpl-1"}, Config(api_url="http://api"))
    sb._session = Mock()
    return sb


def response(status=200, body=None):
    result = requests.Response()
    result.status_code = status
    import json
    result._content = json.dumps(body).encode()
    return result


@pytest.mark.parametrize("limit", [100, 1001, 0, -1])
@pytest.mark.parametrize("infrastructure", [False, True])
def test_expose_wire_and_no_numeric_quota(sandbox, limit, infrastructure):
    sandbox._session.post.return_value = response(body={"publicURL": "https://3000-sb-1.cube.app"})
    assert sandbox.expose_port(3000, port_limit=limit, infrastructure=infrastructure) == "https://3000-sb-1.cube.app"
    sandbox._session.post.assert_called_once_with(
        "http://api/sandboxes/sb-1/ports/3000",
        json={"portLimit": limit, "portClass": "infrastructure" if infrastructure else "user"},
        timeout=(5, 45),
        allow_redirects=False,
    )


def test_default_signature_and_close(sandbox):
    sandbox._session.post.return_value = response(body={"publicURL": "https://service"})
    sandbox.expose_port(3000)
    assert sandbox._session.post.call_args.kwargs["json"] == {"portLimit": 100, "portClass": "user"}
    sandbox._session.delete.return_value = response(204)
    assert sandbox.close_port(3000) is None
    sandbox._session.delete.assert_called_once_with("http://api/sandboxes/sb-1/ports/3000", timeout=(5, 45), allow_redirects=False)


@pytest.mark.parametrize("port", [False, True, 3.0, "3000", None, 0, -1, 65536, 17300, 17301, 49983, 49999])
def test_invalid_and_reserved_ports_never_send(sandbox, port):
    for infrastructure in [False, True]:
        with pytest.raises(ValueError):
            sandbox.expose_port(port, infrastructure=infrastructure)
    with pytest.raises(ValueError):
        sandbox.close_port(port)
    sandbox._session.post.assert_not_called()
    sandbox._session.delete.assert_not_called()


@pytest.mark.parametrize("kwargs", [{"port_limit": True}, {"port_limit": 1.5}, {"port_limit": "100"},
                                  {"port_limit": 2**63}, {"infrastructure": "false"}])
def test_invalid_options_never_send(sandbox, kwargs):
    with pytest.raises(ValueError):
        sandbox.expose_port(3000, **kwargs)
    sandbox._session.post.assert_not_called()


@pytest.mark.parametrize("status,error", [(401, AuthenticationError), (403, AuthenticationError),
                                         (404, SandboxNotFoundError), (409, ApiError), (500, ApiError), (504, ApiError)])
def test_http_errors_propagate(sandbox, status, error):
    sandbox._session.post.return_value = sandbox._session.delete.return_value = response(status, {"message": "sandbox rejected"})
    with pytest.raises(error):
        sandbox.expose_port(3000)
    with pytest.raises(error):
        sandbox.close_port(3000)
    assert sandbox._session.post.call_count == sandbox._session.delete.call_count == 1


@pytest.mark.parametrize("body", [None, {}, {"ret": {"ret_code": 500}}, {"publicURL": None}, {"publicURL": ""}, {"publicURL": 7}])
def test_malformed_success_not_returned_as_url(sandbox, body):
    sandbox._session.post.return_value = response(body=body)
    with pytest.raises(ApiError):
        sandbox.expose_port(3000)


@pytest.mark.parametrize("status", [200, 202, 301, 307])
def test_close_requires_explicit_no_content(sandbox, status):
    sandbox._session.delete.return_value = response(status, {"ret": {"ret_code": 500}})
    with pytest.raises(ApiError):
        sandbox.close_port(3000)


@pytest.mark.parametrize("error", [requests.Timeout, requests.ConnectionError])
def test_transport_uncertainty_propagates_without_retry(sandbox, error):
    sandbox._session.post.side_effect = sandbox._session.delete.side_effect = error("uncertain")
    with pytest.raises(error):
        sandbox.expose_port(3000)
    with pytest.raises(error):
        sandbox.close_port(3000)
    assert sandbox._session.post.call_count == sandbox._session.delete.call_count == 1
