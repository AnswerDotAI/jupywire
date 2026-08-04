"""Jupyter wire-format sessions: message construction, HMAC signing, and (de)serialization.

Adapted from `jupyter_client.session` (BSD-3-Clause, Copyright (c) Jupyter Development Team;
see the Credits section of the README), trimmed to the surface this ecosystem needs and stripped
of traitlets. Wire-compatible with jupyter_client: same frame layout (`<IDS|MSG>` delimiter, hex
HMAC signature, header/parent/metadata/content JSON, raw buffer frames), same replay protection
("Duplicate Signature"), same error strings. Deliberate simplifications, none of which change
bytes on the wire: header dates stay ISO-8601 strings on deserialize (jupyter_client converts
them to datetimes client-side); no v4-protocol adaptation (our kernels speak 5.3 only); no fork
guard. Socket-facing `send`/`recv` live with the transports (kernmini's `MiniSession` subclass);
the websocket JSON/binary helpers below are the frame layer jupygate's channel protocol uses.
"""

import hashlib, hmac, json, numbers, os, struct, uuid
from binascii import b2a_base64
from collections.abc import Iterable
from datetime import date, datetime, timezone
from fastcore.basics import xdumps, revive_dates


DELIM = b"<IDS|MSG>"
protocol_version = "5.3"


def utcnow() -> datetime: return datetime.now(timezone.utc)


def json_default(obj):
    "JSON serializer mirroring `jupyter_client.jsonutil.json_default`: ISO-8601 Z-suffix datetimes, b64 bytes, listified iterables, coerced numerics."
    if isinstance(obj, datetime):
        if obj.tzinfo is None: obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat().replace("+00:00", "Z")
    if isinstance(obj, date): return obj.isoformat()
    if isinstance(obj, bytes): return b2a_base64(obj, newline=False).decode("ascii")
    if isinstance(obj, Iterable): return list(obj)
    if isinstance(obj, numbers.Integral): return int(obj)
    if isinstance(obj, numbers.Real): return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def json_packer(obj) -> bytes:
    return json.dumps(obj, default=json_default, ensure_ascii=False, allow_nan=False,
        separators=(",", ":")).encode("utf8", errors="surrogateescape")


def extract_header(msg_or_header: dict) -> dict:
    "Given a message or header, return the header."
    if not msg_or_header: return {}
    if "header" in msg_or_header: return msg_or_header["header"]
    if "msg_id" in msg_or_header: return msg_or_header
    raise KeyError("no header found")


def _bytes(m) -> bytes:
    "A frame as bytes, whether it arrived as bytes or a non-copying frame (e.g. zmq.Frame), without importing any transport."
    return m if isinstance(m, bytes) else m.bytes


class Session:
    "Build, sign, serialize, and deserialize Jupyter protocol messages over one session identity."

    def __init__(self, key: bytes | None = None, signature_scheme: str = "hmac-sha256", username: str = "",
        session: str | None = None, digest_history_size: int = 2**16):
        self.session = session or str(uuid.uuid4())
        self.username = username or os.environ.get("USER", "username")
        self.metadata = {}
        if key is None: key = uuid.uuid4().hex.encode("ascii")  # jupyter_client parity: a fresh random key when unspecified
        if key:
            if not signature_scheme.startswith("hmac-"): raise ValueError(f"unsupported signature scheme: {signature_scheme!r}")
            digest = signature_scheme.removeprefix("hmac-")
            if digest not in hashlib.algorithms_available: raise ValueError(f"unsupported digest: {digest!r}")
            self.auth = hmac.HMAC(key, digestmod=getattr(hashlib, digest))
        else: self.auth = None
        self.digest_history = set()
        self.digest_history_size = digest_history_size
        self.message_count = 0
        self.pack, self.unpack = json_packer, json.loads

    @property
    def msg_id(self) -> str:
        n = self.message_count
        self.message_count += 1
        return f"{self.session}_{os.getpid()}_{n}"

    @property
    def bsession(self) -> bytes: return self.session.encode("ascii")

    def msg_header(self, msg_type: str) -> dict:
        return dict(msg_id=self.msg_id, msg_type=msg_type, username=self.username, session=self.session,
            date=utcnow(), version=protocol_version)

    def msg(self, msg_type: str, content: dict | None = None, parent: dict | None = None,
        header: dict | None = None, metadata: dict | None = None) -> dict:
        "Return the nested message dict (the pre-wire form; `serialize` makes the frame list)."
        header = self.msg_header(msg_type) if header is None else header
        parent_h = {} if parent is None else extract_header(parent)
        msg = dict(header=header, msg_id=header["msg_id"], msg_type=header["msg_type"], parent_header=parent_h,
            content=content or {}, metadata=self.metadata.copy())
        if metadata is not None: msg["metadata"].update(metadata)
        return msg

    def sign(self, msg_list: list) -> bytes:
        "Hex HMAC digest of the [p_header,p_parent,p_metadata,p_content] frames; b'' when unauthenticated."
        if self.auth is None: return b""
        h = self.auth.copy()
        for m in msg_list: h.update(_bytes(m))
        return h.hexdigest().encode()

    def serialize(self, msg: dict, ident: list[bytes] | bytes | None = None) -> list[bytes]:
        "Frame list [ident..., DELIM, signature, p_header, p_parent, p_metadata, p_content] for `msg`."
        content = msg.get("content", {})
        if content is None: content = self.pack({})
        elif isinstance(content, dict): content = self.pack(content)
        elif isinstance(content, str): content = content.encode("utf8")
        elif not isinstance(content, bytes): raise TypeError(f"content incorrect type: {type(content)}")
        real_message = [self.pack(msg["header"]), self.pack(msg["parent_header"]), self.pack(msg["metadata"]), content]
        to_send = list(ident) if isinstance(ident, list) else [ident] if ident is not None else []
        to_send.append(DELIM)
        to_send.append(self.sign(real_message))
        to_send.extend(real_message)
        return to_send

    def feed_identities(self, msg_list: list, copy: bool = True) -> tuple[list[bytes], list]:
        "Split a received frame list at DELIM into (idents, remainder)."
        for idx, m in enumerate(msg_list):
            if _bytes(m) == DELIM: return [bytes(_bytes(i)) for i in msg_list[:idx]], msg_list[idx + 1:]
        raise ValueError("DELIM not in msg_list")

    def _add_digest(self, signature: bytes):
        if self.digest_history_size == 0: return
        self.digest_history.add(signature)
        if len(self.digest_history) > self.digest_history_size:
            import random
            to_cull = random.sample(tuple(sorted(self.digest_history)), len(self.digest_history) // 10)
            self.digest_history.difference_update(to_cull)

    def deserialize(self, msg_list: list, content: bool = True, copy: bool = True) -> dict:
        "Verify signature and unpack a [signature, p_header, p_parent, p_metadata, p_content, buffer...] frame list."
        msg_list = [_bytes(m) for m in msg_list[:5]] + list(msg_list[5:])
        if self.auth is not None:
            signature = msg_list[0]
            if not signature: raise ValueError("Unsigned Message")
            if signature in self.digest_history: raise ValueError("Duplicate Signature: %r" % signature)
            if content: self._add_digest(signature)  # only record when unpacking, not peeking
            if not hmac.compare_digest(signature, self.sign(msg_list[1:5])): raise ValueError("Invalid Signature: %r" % signature)
        if len(msg_list) < 5: raise TypeError("malformed message, must have at least 5 elements")
        header = self.unpack(msg_list[1])
        content_v = self.unpack(msg_list[4]) if content else msg_list[4]
        bufs = [memoryview(_bytes(b)) for b in msg_list[5:]]
        return dict(header=header, msg_id=header["msg_id"], msg_type=header["msg_type"], parent_header=self.unpack(msg_list[2]),
            metadata=self.unpack(msg_list[3]), content=content_v, buffers=bufs)


def dumps(msg): return xdumps(msg)

def loads(s: str):
    msg = json.loads(s)
    if isinstance(msg, dict):
        for k in ("header", "parent_header"):
            if isinstance(msg.get(k), dict): msg[k] = revive_dates(msg[k])
    return msg


def pack_frames(body:bytes, buffers=())->bytes:
    "Pack `body` and binary `buffers` into a legacy Jupyter websocket binary frame"
    parts = [body, *(bytes(b) for b in buffers)]
    offs = [4*(len(parts)+1)]
    for p in parts[:-1]: offs.append(offs[-1]+len(p))
    return b''.join([struct.pack(f'!{len(parts)+1}I', len(parts), *offs), *parts])

def unpack_frames(bmsg:bytes)->tuple:
    "Split a legacy Jupyter websocket binary frame into `(body, buffers)`"
    n = struct.unpack('!I', bmsg[:4])[0]
    offs = [*struct.unpack(f'!{n}I', bmsg[4:4*(n+1)]), None]
    parts = [bmsg[a:b] for a,b in zip(offs[:-1], offs[1:])]
    return parts[0], parts[1:]


def serialize_binary_message(msg):
    msg = msg.copy()
    buffers = msg.pop("buffers")
    return pack_frames(dumps(msg).encode("utf8"), buffers)


def deserialize_binary_message(bmsg: bytes):
    body, bufs = unpack_frames(bmsg)
    msg = loads(body.decode("utf8"))
    msg["buffers"] = [memoryview(b) for b in bufs]
    return msg


def validate_string_dict(dct: dict[str, str]) -> None:
    """Validate that the input is a dict with string keys and values.

    Raises ValueError if not."""
    for k, v in dct.items():
        if not isinstance(k, str): raise ValueError("key %r in dict must be a string" % k)
        if not isinstance(v, str): raise ValueError("value %r in dict must be a string" % v)
