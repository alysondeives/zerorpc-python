from __future__ import absolute_import
import eventlet

import zerorpc
from .testutils import teardown, random_ipc_endpoint

def test_client_connect():
    endpoint = random_ipc_endpoint()

    class MySrv(zerorpc.Server):

        def echo(self, *args, **kwargs):
            return args, kwargs

    srv = MySrv()
    srv.bind(endpoint)
    eventlet.spawn(srv.run)

    client = zerorpc.Client()
    client.connect(endpoint)

    args = 1, 2, 3
    kwargs = {'a': 7, 'b': 8}
    res = client.echo(*args, **kwargs)
    assert len(res) == 2
    assert res[0] == args
    assert len(res[1]) == 3
    assert 'a' in res[1] and 'b' in res[1]

def test_client_quick_connect():
    endpoint = random_ipc_endpoint()

    class MySrv(zerorpc.Server):

        def echo(self, *args, **kwargs):
            return args, kwargs

    srv = MySrv()
    srv.bind(endpoint)
    eventlet.spawn(srv.run)

    client = zerorpc.Client(endpoint)

    args = 1, 2, 3
    kwargs = {'a': 7, 'b': 8}
    res = client.echo(*args, **kwargs)
    assert len(res) == 2
    assert res[0] == args
    assert len(res[1]) == 3
    assert 'a' in res[1] and 'b' in res[1]
