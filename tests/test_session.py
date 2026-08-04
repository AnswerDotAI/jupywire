"Wire-format tests, cross-verified against jupyter_client (dev dep): same bytes, same signatures, same errors."
import json, os, stat, struct
from datetime import datetime, timezone, date

import pytest
from jupyter_client.session import Session as JCSession

from jupywire.session import (Session, json_default, json_packer, serialize_binary_message,
    deserialize_binary_message, validate_string_dict, dumps, loads, DELIM)
from jupywire.connect import write_connection_file

KEY = b"secret-key"

def _wire(sess, msg, ident=None):
    frames = sess.serialize(msg, ident=ident)
    idents, rest = sess.feed_identities(frames)
    return idents, list(rest)

def test_sign_matches_jupyter_client():
    s, jc = Session(key=KEY), JCSession(key=KEY)
    frames = [b'{"a":1}', b'{}', b'{}', b'{"code":"6*7"}']
    assert s.sign(frames) == jc.sign(frames)
    assert Session(key=b"").sign(frames) == b""


def test_roundtrip_via_jupyter_client():
    s, jc = Session(key=KEY, session="abc"), JCSession(key=KEY, session="abc")
    msg = s.msg("execute_request", dict(code="6*7", silent=False))
    wire = s.serialize(msg)
    assert wire[0] == DELIM
    idents, rest = jc.feed_identities(wire)
    got = jc.deserialize(rest)
    assert got["content"] == msg["content"] and got["msg_type"] == "execute_request"
    # and the reverse: jupyter_client-built frames verify and unpack here
    jmsg = jc.msg("kernel_info_request")
    idents, rest = s.feed_identities(jc.serialize(jmsg))
    got = s.deserialize(rest)
    assert got["msg_id"] == jmsg["header"]["msg_id"]



def test_json_default_parity():
    from jupyter_client.jsonutil import json_default as jc_default
    aware = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    for v in (aware, date(2026, 1, 2), b"some bytes", {1, 2, 3}): assert json_default(v) == jc_default(v)
    naive = datetime(2026, 1, 2, 3, 4, 5)
    assert json_default(naive) == "2026-01-02T03:04:05Z"  # naive treated as UTC (jupyter_client's local-time reading is deprecated)
    with pytest.raises(TypeError): json_default(object())
    assert json.loads(json_packer(dict(b=b"xy", t=aware)).decode()) == json.loads(
        json.dumps(dict(b=b"xy", t=aware), default=jc_default))


def test_default_key_is_random_like_jupyter_client():
    a, b = Session(), Session()
    assert a.auth is not None and b.auth is not None
    assert a.sign([b"x"]) != b.sign([b"x"])
    assert Session(session="abc").bsession == b"abc"


def test_binary_message_roundtrip():
    s = Session(key=KEY)
    msg = s.msg("display_data", dict(data={"text/plain": "hi"}))
    msg["buffers"] = [b"bufone", b"buftwo"]
    frame = serialize_binary_message(msg)
    n = struct.unpack("!I", frame[:4])[0]
    assert n == 3  # body + two buffers: the legacy Jupyter websocket layout is part count, offset table, then parts
    got = deserialize_binary_message(frame)
    assert got["content"] == msg["content"]
    assert [bytes(b) for b in got["buffers"]] == [b"bufone", b"buftwo"]
    assert loads(dumps(s.msg("status", dict(execution_state="idle"))))["content"]["execution_state"] == "idle"


def test_validate_string_dict():
    validate_string_dict(dict(a="b"))
    with pytest.raises(ValueError): validate_string_dict({"a": 1})
    with pytest.raises(ValueError): validate_string_dict({1: "a"})


def test_write_connection_file(tmp_path):
    fname = str(tmp_path/"kernel.json")
    fname2, cfg = write_connection_file(fname, ip="127.0.0.1", key=b"deadbeef")
    assert fname2 == fname and os.path.exists(fname)
    assert stat.S_IMODE(os.stat(fname).st_mode) == 0o600
    on_disk = json.loads(open(fname).read())
    ports = [cfg[k] for k in ("shell_port", "iopub_port", "stdin_port", "control_port", "hb_port")]
    assert len(set(ports)) == 5 and all(p > 0 for p in ports)
    assert on_disk == cfg
    assert cfg["key"] == "deadbeef" and cfg["transport"] == "tcp" and cfg["ip"] == "127.0.0.1"
    from jupyter_client.connect import write_connection_file as jc_wcf
    _, jc_cfg = jc_wcf(str(tmp_path/"jc.json"), ip="127.0.0.1", key=b"deadbeef")
    assert set(cfg) == set(jc_cfg)


def test_content_coercions():
    "Bytes b64-encode, dates ISO-format, and iterables listify at pack time, as jupyter_client does (image display bundles rely on this)"
    from binascii import b2a_base64
    from datetime import date
    s = Session(key=b"k")
    png = b"\x89PNG fake bytes"
    msg = s.msg("display_data", dict(data={"image/png": png}, when=date(2026, 8, 4), tags={"a"}))
    _, rest = _wire(s, msg)
    c = s.deserialize(rest)["content"]
    assert c["data"]["image/png"] == b2a_base64(png, newline=False).decode("ascii")
    assert c["when"] == "2026-08-04"
    assert c["tags"] == ["a"]
    jcs = pytest.importorskip("jupyter_client.session")
    js = jcs.Session(key=b"k")
    _, jrest = js.feed_identities(js.serialize(js.msg("display_data", msg["content"])))
    assert js.deserialize(jrest)["content"] == c


def test_replay_tamper_unsigned():
    s = Session(key=b"secret")
    _, rest = _wire(s, s.msg("a_request", dict(x=1)))
    s.deserialize(list(rest))
    with pytest.raises(ValueError, match="Duplicate Signature"): s.deserialize(list(rest))
    bad = list(rest)
    bad[0], bad[4] = b"0" * len(bad[0]), b'{"x":2}'
    with pytest.raises(ValueError, match="Invalid Signature"): s.deserialize(bad)
    with pytest.raises(ValueError, match="Unsigned Message"): s.deserialize([b""] + list(rest)[1:])


def test_parent_and_buffers():
    s = Session(key=b"k")
    parent = s.msg("a_request", {})
    reply = s.msg("a_reply", dict(status="ok"), parent=parent)
    frames = s.serialize(reply) + [b"rawbuf"]
    _, rest = s.feed_identities(frames)
    out = s.deserialize(rest)
    assert out["parent_header"]["msg_id"] == parent["header"]["msg_id"]
    assert bytes(out["buffers"][0]) == b"rawbuf"
