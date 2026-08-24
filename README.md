# jupywire

Minimal Jupyter protocol commons: wire-format sessions (message construction, HMAC signing, frame serialize/deserialize) and kernel-client call conventions, shared by kernmini, jupygate, jupyasyncclient, and conkernelclient.

## What's here

- `jupywire.session`: `Session` — Jupyter protocol messages: construction, HMAC signing, frame (de)serialization, replay protection — plus the websocket JSON/binary frame helpers (`dumps`/`loads`, `serialize_binary_message`/`deserialize_binary_message`) and `validate_string_dict`.
- `jupywire.connect`: `write_connection_file` — kernel connection files with random free-port selection, written 0600.
- `jupywire.route`: `RouterOps` and `JmsgQueues` — message handling for kernel clients. `reply()` awaits one `execute_reply`; `run()` collects every message one execute causes, streams through `on_output`, and turns each `on_stdin` return value into the correctly parented `input_reply` (stdin is disabled when that hook is absent). Everything unmatched goes to the app's `on_jmsg` callback, with `JmsgQueues` as the pull adapter. A generic `request` sends any named protocol request, with `shell`/`control` sugar and the typed verbs (`complete`, `inspect`, `check`, `history`, `comm_msg`) on top. The transport supplies `execute`, `send`, a `session`, and a read loop feeding `route`. DESIGN.md states the full contract; `tests/test_route.py` is the conformance suite both clients inherit.
- `jupywire.ops`: `EvalOps` — a mixin giving any kernel client the calling conventions (`eval`, `ipy`, the `ipyfuncs` service methods, `retr`, `eval_expr`/`user_exprs`, and the sync fire-and-forget `xpush`/`xenv`) over two abstract methods supplied by the transport: `reply`, which awaits the shell reply, and `execute`, which sends without awaiting one.

## Install

```bash
pip install jupywire
```


## Credits

`jupywire.session` and `jupywire.connect` are adapted from [jupyter_client](https://github.com/jupyter/jupyter_client) (`jupyter_client.session`, `jupyter_client.connect`, `jupyter_client.client`), Copyright (c) Jupyter Development Team, distributed under the terms of the Modified BSD License (BSD-3-Clause); see that project's COPYING.md.

The adaptations are trimmed, traitlets-free mirrors that stay wire-compatible. Divergences are noted in each module's docstring.
