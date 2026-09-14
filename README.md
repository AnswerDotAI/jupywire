# jupywire

`jupywire` provides shared Jupyter protocol code for kernmini, jupygate, jupyasyncclient, and conkernelclient. It handles wire-format messages, connection files, and common kernel-client operations.

## What's here

The package has four modules:

- `jupywire.session` provides `Session` for constructing and HMAC-signing Jupyter messages. It serializes and deserializes frames and provides replay protection. The module also includes websocket JSON helpers (`dumps` and `loads`), binary helpers (`serialize_binary_message` and `deserialize_binary_message`), and `validate_string_dict`.
- `jupywire.connect` provides `write_connection_file`. It selects random free ports and writes a kernel connection file with permissions `0600`.
- `jupywire.route` provides `RouterOps` for routing kernel-client messages and `JmsgQueues` for queue-based access to them.
- `jupywire.ops` provides `EvalOps`, a mixin for evaluation and variable operations on kernel clients.

## Install

```bash
pip install jupywire
```

## Client messages

`RouterOps` provides these ways to receive execution results:

- `reply()` awaits one `execute_reply`.
- `run()` sends the execution request when called. It returns an async generator of every message caused by that execution.
- `exec_outs` collects the results from `run` and returns a list of rendered results.

`run` uses each return value from `on_stdin` to send an `input_reply` with the correct parent. `allow_stdin` defaults to whether an `on_stdin` hook is present. Pass `allow_stdin=True` without a hook to handle prompts through the application's `on_jmsg` callback.

Every inbound message also goes once to the application's `on_jmsg` callback, in receive order, after request routing. This includes collected run messages and stdin handled by a run callback. Receiving a notification does not transfer ownership of that stdin exchange. Keep the handler a cheap dispatcher; an awaitable return blocks the transport reader until it completes. Use `JmsgQueues` to pull the same messages from queues.

`request` sends any named protocol request. `shell` and `control` provide channel-specific helpers. Named methods include `complete`, `inspect`, `check`, `history`, and `comm_msg`.

The transport must supply `execute`, `send`, and a `session`. Its read loop passes incoming messages to `route`. [DESIGN.md](DESIGN.md) specifies the full contract. Both clients inherit the conformance suite in `tests/test_route.py`.

## Evaluation and variables

`EvalOps` provides `eval`, `ipy`, the `ipyfuncs` service methods, `retr`, `eval_expr`, and `user_exprs`. It also provides `xpush` and `xenv` for synchronous fire-and-forget calls.

These operations use the client's `reply` or `execute` method. `reply` waits for a shell reply. `execute` sends without awaiting a reply.

Pass `sidecar_=True` to evaluate in the persistent sidecar. kernmini creates a missing named subshell on first use. Variable operations use this serial sidecar by default. Ordinary `eval` calls use the main shell by default.

## Credits

`jupywire.session` and `jupywire.connect` are adapted from [jupyter_client](https://github.com/jupyter/jupyter_client), specifically `jupyter_client.session`, `jupyter_client.connect`, and `jupyter_client.client`. Copyright (c) Jupyter Development Team. The source is distributed under the Modified BSD License (BSD-3-Clause). See that project's COPYING.md.

The adaptations remove traitlets and retain wire compatibility. Each module's docstring records its differences from the original.
