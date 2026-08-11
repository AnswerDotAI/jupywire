# jupywire

Minimal Jupyter protocol commons: wire-format sessions (message construction, HMAC signing, frame serialize/deserialize) and kernel-client call conventions, shared by kernmini, jupygate, jupyasyncclient, and conkernelclient.

## What's here

- `jupywire.session`: `Session` — Jupyter protocol messages: construction, HMAC signing, frame (de)serialization, replay protection — plus the websocket JSON/binary frame helpers (`dumps`/`loads`, `serialize_binary_message`/`deserialize_binary_message`) and `validate_string_dict`.
- `jupywire.connect`: `write_connection_file` — kernel connection files with random free-port selection, written 0600.
- `jupywire.ops`: `EvalOps` — a mixin giving any kernel client the calling conventions (`eval`, `ipy`, the `ipyfuncs` service methods, `retr`, and the sync fire-and-forget `xpush`/`xenv`) over two abstract methods supplied by the transport: `reply`, which awaits the shell reply, and `execute`, which sends without awaiting one.

## Install

```bash
pip install jupywire
```


## Credits

`jupywire.session` and `jupywire.connect` are adapted from [jupyter_client](https://github.com/jupyter/jupyter_client) (`jupyter_client.session`, `jupyter_client.connect`, `jupyter_client.client`), Copyright (c) Jupyter Development Team, distributed under the terms of the Modified BSD License (BSD-3-Clause); see that project's COPYING.md.

The adaptations are trimmed, traitlets-free mirrors that stay wire-compatible. Divergences are noted in each module's docstring.
