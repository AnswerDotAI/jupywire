"""Kernel connection files: `write_connection_file` with random free-port selection.

Adapted from `jupyter_client.connect` (BSD-3-Clause, Copyright (c) Jupyter Development Team; see
the Credits section of the README). Trimmed: no CurveZMQ key support, `ip` defaults to 127.0.0.1
rather than probing local interfaces, and `secure_write` is inlined as an 0600 open.
"""

import json, os, socket, stat, tempfile


def _secure_open(fname: str):
    "Open `fname` for writing, created 0600 (the file carries the kernel's signing key)."
    return os.fdopen(os.open(fname, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w")

def write_connection_file(fname=None, shell_port=0, iopub_port=0, stdin_port=0, hb_port=0, control_port=0,
    ip="", key=b"", transport="tcp", signature_scheme="hmac-sha256", kernel_name="", **kwargs) -> tuple[str, dict]:
    "Write a Jupyter connection-file JSON dict to `fname` (0600), picking random free ports for any left 0; returns `(fname, cfg)`."
    if not ip: ip = "127.0.0.1"
    if not fname:
        fd, fname = tempfile.mkstemp(".json")
        os.close(fd)
    ports, sockets = [], []
    ports_needed = sum(int(p <= 0) for p in (shell_port, iopub_port, stdin_port, control_port, hb_port))
    if transport == "tcp":
        for _ in range(ports_needed):
            sock = socket.socket()
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\0" * 8)  # 8 null bytes = struct.pack('ii', (0,0))
            sock.bind((ip, 0))
            sockets.append(sock)
        for sock in sockets:
            port = sock.getsockname()[1]
            sock.close()
            ports.append(port)
    else:  # ipc: sequentially numbered paths that don't exist yet
        N = 1
        for _ in range(ports_needed):
            while os.path.exists(f"{ip}-{N!s}"): N += 1
            ports.append(N)
            N += 1
    if shell_port <= 0: shell_port = ports.pop(0)
    if iopub_port <= 0: iopub_port = ports.pop(0)
    if stdin_port <= 0: stdin_port = ports.pop(0)
    if control_port <= 0: control_port = ports.pop(0)
    if hb_port <= 0: hb_port = ports.pop(0)
    cfg = dict(shell_port=shell_port, iopub_port=iopub_port, stdin_port=stdin_port, control_port=control_port,
        hb_port=hb_port, ip=ip, key=key.decode(), transport=transport, signature_scheme=signature_scheme, kernel_name=kernel_name)
    cfg.update(kwargs)
    with _secure_open(fname) as f: f.write(json.dumps(cfg, indent=2))
    if hasattr(stat, "S_ISVTX"):
        # sticky-bit the parent dir, so only the owner can remove the file (EPERM etc suppressed: we may not own the dir)
        runtime_dir = os.path.dirname(fname)
        if runtime_dir:
            try: os.chmod(runtime_dir, os.stat(runtime_dir).st_mode | stat.S_ISVTX)
            except OSError: pass
    return fname, cfg
