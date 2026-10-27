"Wire interoperability, websocket framing, and real connection-file stories."
import json, stat, struct
from datetime import datetime, timezone, date

import pytest
from jupyter_client import BlockingKernelClient
from jupyter_client.session import Session as JCSession

from jupywire.session import Session, to_frame, from_frame, DELIM
from jupywire.connect import write_connection_file


def test_authenticated_exchange_story():
    "Exchange signed messages both ways with jupyter_client, including rich content and binary buffers."
    ours, peer = Session(key=b'secret'), JCSession(key=b'secret')
    content = dict(data={'image/png': b'xy'}, when=date(2026, 1, 2), tags={'a'}, at=datetime(2026, 1, 2, tzinfo=timezone.utc))
    request = ours.msg('display_data', content, metadata=dict(cell='one'))
    frames = ours.serialize(request, ident=[b'router', b'kernel']) + [b'raw buffer']
    assert frames[2] == DELIM and ours.sign(frames[4:8]) == peer.sign(frames[4:8])
    ids, body = peer.feed_identities(frames)
    got = peer.deserialize(body)
    assert ids == [b'router', b'kernel'] and got['metadata'] == dict(cell='one')
    assert got['content'] == dict(data={'image/png': 'eHk='}, when='2026-01-02', tags=['a'], at='2026-01-02T00:00:00Z')
    assert bytes(got['buffers'][0]) == b'raw buffer'
    reply = peer.msg('display_reply', content, parent=got)
    ids, body = ours.feed_identities(peer.serialize(reply, ident=b'kernel') + [b'return buffer'])
    received = ours.deserialize(body)
    assert ids == [b'kernel'] and received['parent_header']['msg_id'] == request['msg_id']
    assert received['content'] == got['content'] and bytes(received['buffers'][0]) == b'return buffer'
    with pytest.raises(ValueError, match='Duplicate Signature'): ours.deserialize(body)
    _, fresh = ours.feed_identities(peer.serialize(peer.msg('another_reply', dict(x=1))))
    with pytest.raises(ValueError, match='Invalid Signature'): ours.deserialize([*fresh[:4], b'{"x":2}'])
    with pytest.raises(ValueError, match='Unsigned Message'): ours.deserialize([b'', *fresh[1:]])
    assert Session().sign(frames[4:8]) != Session().sign(frames[4:8])
    unsigned = Session(key=b'')
    _, body = unsigned.feed_identities(unsigned.serialize(unsigned.msg('status', dict(execution_state='idle'))))
    assert body[0] == b'' and JCSession(key=b'').deserialize(body)['content']['execution_state'] == 'idle'


def test_websocket_framing_story():
    "A text frame revives header dates; a message with buffers becomes a binary frame that keeps the body and each opaque buffer separate."
    session = Session()
    msg = session.msg('display_data', dict(data={'text/plain': 'hi'}), parent=session.msg('execute_request'))
    text = to_frame(msg)
    assert isinstance(text, str) and from_frame(text) == dict(msg, buffers=[])
    msg['buffers'] = [b'first', b'\0\xffsecond']
    frame = to_frame(msg)
    count, start, first, second = struct.unpack('!4I', frame[:16])
    assert (count, start) == (3, 16)
    assert json.loads(frame[start:first])['content'] == msg['content']
    assert [frame[first:second], frame[second:]] == msg['buffers']
    received = from_frame(frame)
    assert received['header'] == msg['header'] and received['parent_header'] == msg['parent_header']
    assert received['content'] == msg['content'] and [bytes(b) for b in received['buffers']] == msg['buffers']


def test_connection_file_story(tmp_path):
    "The persisted credentials and allocated ports load directly into a real jupyter_client."
    path = tmp_path/'kernel.json'
    fname, cfg = write_connection_file(str(path), key=b'secret')
    assert fname == str(path) and stat.S_IMODE(path.stat().st_mode) == 0o600 and json.loads(path.read_text()) == cfg
    client = BlockingKernelClient(connection_file=fname)
    client.load_connection_file()
    names = ('shell_port', 'iopub_port', 'stdin_port', 'control_port', 'hb_port')
    ports = [cfg[name] for name in names]
    assert len(set(ports)) == 5 and all(port > 0 for port in ports)
    assert [getattr(client, name) for name in names] == ports
    assert client.ip == '127.0.0.1' and client.transport == 'tcp' and client.session.key == b'secret'
