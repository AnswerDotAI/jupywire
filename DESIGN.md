# Message handling in jupywire kernel clients

Agreed 2026-08-23. This is the target design for `jupywire.route` and its two clients, jupyasyncclient (websocket) and conkernelclient (zmq). It replaces `RouterOps`, `Run`, and `claim`. Each mechanism below appears at the point where the problem it solves appears.

## The wire

A kernel and its client exchange messages on four channels. shell carries requests and their replies. control carries requests that must not queue behind shell. iopub is a broadcast of everything the kernel does while it works. stdin carries the kernel's questions to the user.

Over zmq, each channel is one socket. A fifth socket carries the heartbeat. The heartbeat is not message traffic, and it appears in this design only as a death signal. Over a websocket, every channel shares one socket, and each JSON frame names its channel in a `channel` key. rustygate adds one nonstandard channel named `cells`, which carries file cell-op broadcasts rather than kernel protocol traffic.

The transports converge before this design begins. conkernelclient runs one `_pump` task per zmq socket. Each `_pump` stamps the channel name onto the message dict. jupyasyncclient runs one `_recv_loop` task on the websocket. Both hand every inbound message to one function, `route(msg)`. `route()` reads `channel` and `msg_type` at the top level of the dict, filling `msg_type` from `header` when a transport omits it. The rest of this document defines `route()` and the functions that feed it.

## execute()

`execute(code, msg_id=...)` builds an `execute_request`, sends it on shell, and returns the msg_id. It does nothing else. It has no `reply=` parameter. This design deletes today's `reply=` branches in both transports.

The caller may pass its own msg_id, because the kernel treats the msg_id as an opaque string. The kernel copies the whole request header into `parent_header` on every message the request causes. The msg_id is therefore a tag the client controls. Solveit and ipyai tag each cell execute with `{cell_id}.{token}`, and every message that cell produces comes back naming its cell.

Fire and forget is the idiomatic use for UI cells. The app sends the tagged execute and returns to its event loop. The cell's outputs, statuses, and reply arrive later, through the `on_jmsg` callback described below. The app never awaits a cell's reply.

## reply()

Some requests exist for their reply. `eval` sends an execute whose result comes back inside the `execute_reply`, in `user_expressions`. The caller wants that one message and nothing else.

`reply(code)` sends the execute and returns an awaitable of its `execute_reply`:

```python
def reply(self, code, timeout=None, msg_id=None, **kw):
    mid = msg_id or self.new_msg_id()
    fut = self.replies[mid] = asyncio.get_running_loop().create_future()
    self.execute(code, msg_id=mid, **kw)
    async def _wait():
        try: return await asyncio.wait_for(fut, timeout)
        finally: self.replies.pop(mid, None)
    return _wait()
```

`self.replies` maps msg_id to future. `route()` fills the future when the matching reply arrives. The `finally` pops the entry however the wait ends, so a timeout or cancellation leaves no entry behind. The `msg_id` parameter serves the same tagging rule as `execute`'s. `EvalOps.eval` already passes one through.

`reply()` is a sync function that sends at call time and returns an awaitable. A caller can send now and collect later by holding the awaitable, which an `async def` cannot express. The cost is the abandoned call. A caller that never awaits has already sent. Its entry stays until the reply arrives, the kernel dies, or the client closes. All three paths clean the entry up.

## A first route()

```python
def route(self, msg):
    if msg['channel'] in ('shell', 'control'):
        fut = self.replies.pop(msg.get('parent_header', {}).get('msg_id'), None)
        if fut is not None and not fut.done(): return fut.set_result(msg)
    self.on_jmsg(msg)
```

Two destinations. A shell or control message whose parent msg_id has a waiting future resolves it. Everything else goes to one app-supplied callback, `on_jmsg`. The name jmsg avoids collision with dialog messages. Solveit's `process_jmsg` is such a callback today. It reads the parent msg_id, splits at the dot, finds the cell, and folds the message into it.

The channel guard matters, because the execute behind a `reply()` also causes iopub traffic parented to the same msg_id, and the busy status usually arrives before the reply. Only a shell or control message is a reply, so only those may resolve the future.

The `done()` guard exists because `set_result` raises `InvalidStateError` on a future already cancelled by its timeout. A reply that arrives after its `reply()` timed out finds no entry, or finds a cancelled future, and flows to `on_jmsg` like any other unmatched message.

## run()

Tool calls need a third pattern. Most tools run as silent executes, where ipyai's `KernelBridge` sends the tool call with `reply()` and reads the result back through `user_expressions`. The `py` tool cannot, because its result is whatever the code printed or displayed, and those arrive as iopub messages. The same need appears in clikernel, teleprint, and test helpers. Each runs one piece of code and wants its outputs back.

`run(code)` returns a list of the messages parented to its execute, up to and including the `execute_reply` and the idle status:

```python
async def run(self, code, timeout=None, msg_id=None, **kw):
    mid = msg_id or self.new_msg_id()
    fut = asyncio.get_running_loop().create_future()
    self.runs[mid] = [[], fut, False, False]      # msgs, fut, got_reply, got_idle
    try:
        self.execute(code, msg_id=mid, **kw)
        return await asyncio.wait_for(fut, timeout)
    finally: self.runs.pop(mid, None)
```

`self.runs` maps msg_id to `[msgs, fut, got_reply, got_idle]`. `route()` appends every parented message to `msgs`, from shell, control, and iopub alike. stdin is the one exception, and its section below states why. The two booleans track completion. The reply is the last shell message of a request. Idle is the last iopub message. The reply and the idle status travel on different channels, and nothing orders delivery across channels, so completion requires both.

`run()` is an `async def` and sends only when awaited. An unawaited `run()` sends nothing and files nothing. This differs from `reply()` because no `run()` caller separates send time from collect time. Concurrent tool calls compose with `asyncio.gather`. Each call has its own entry, so concurrent runs collect independently. There is no lock. No run can see another run's messages.

`route()` grows one branch, pulled out as a helper:

```python
def route(self, msg):
    parent = msg.get('parent_header', {}).get('msg_id')
    if (r := self.runs.get(parent)) is not None: return self._run_msg(parent, r, msg)
    if msg['channel'] in ('shell', 'control'):
        if (fut := self.replies.pop(parent, None)) is not None and not fut.done(): return fut.set_result(msg)
    self.on_jmsg(msg)

def _run_msg(self, parent, r, msg):
    "Fold one message into its run(): collect, complete on reply plus idle"
    msgs, fut, _, _ = r
    msgs.append(msg)
    mt = msg['msg_type']
    if mt == 'execute_reply': r[2] = True
    if mt == 'status' and msg['content'].get('execution_state') == 'idle': r[3] = True
    if r[2] and r[3]:
        self.runs.pop(parent)
        if not fut.done(): fut.set_result(msgs)
```

`route()` pops the entry as it resolves, so a message parented to a finished run flows to `on_jmsg` like any other unmatched message. Output printed by a background thread after idle reaches the app rather than a dead entry.

Most callers of `run()` want rendered outputs rather than raw messages. `exec_outs(code)` awaits `run` and keeps only the output messages, converted to nbformat form by fastcore.nbio's `msg2out`:

```python
async def exec_outs(self, code, **kw):
    return [msg2out(m) for m in await self.run(code, **kw) if m['msg_type'] in OUTPUT_MSGS]
```

`OUTPUT_MSGS` is `('stream', 'display_data', 'execute_result', 'error')`. clikernel's `execute_outs` becomes one call to `exec_outs`.

## Interrupts and kernel death

An interrupt needs no mechanism. It makes the running execute finish early with an error, and the reply and the idle status still arrive, so every waiter completes normally.

Death is different, because a dead kernel sends nothing, and a waiter with no timeout would sleep forever. Deleting the dict entries would not wake anyone, because each awaiting coroutine holds its own reference to its future. A future wakes its awaiter only on `set_result`, `set_exception`, or `cancel`. The death handler must therefore fail every future, and only then clear the dicts.

Over the websocket, death arrives as one more inbound message. rustygate runs each kernel's heartbeat itself. `GET /api/kernels/{id}` returns the result in two model fields. `execution_state` is one of `alive`, `unresponsive`, `restarting`, or `dead`. `last_heartbeat` is the time of the last echo. Three missed heartbeats set the model's `execution_state` to `unresponsive`. The next echo sets it back to `alive`. rustygate never kills an unresponsive kernel. rustygate broadcasts neither the `unresponsive` transition nor the recovery. When the kernel process exits, rustygate sets the model's `execution_state` to `dead` and broadcasts a synthesized iopub `status` message whose `content.execution_state` is `dead`. The synthesized `status` message is the only death signal a websocket client receives.

`route()` therefore checks for that message before anything else:

```python
def route(self, msg):
    if msg['msg_type'] == 'status' and msg['content'].get('execution_state') == 'dead': return self._kernel_died(msg)
    ...

def _kernel_died(self, msg):
    "Fail every waiter and run, then let the status reach the app"
    exc = DeadKernelError('kernel died')
    for fut in self.replies.values():
        if not fut.done(): fut.set_exception(exc)
    for _, fut, *_ in self.runs.values():
        if not fut.done(): fut.set_exception(exc)
    self.replies.clear()
    self.runs.clear()
    return self.on_jmsg(msg)
```

Every `reply()` and `run()` caller wakes holding `DeadKernelError`. The status then reaches `on_jmsg`, so the app can show the kernel as dead. No app polls for liveness.

Over zmq there is no gateway, so the dead status never arrives on the wire. conkernelclient produces the status itself, from two signals. A `_pump` that hits a socket error has lost the transport. jupyter_client's `HBChannel` covers the kernel process itself. It echoes every `time_to_dead` seconds (default 1.0) and reports the result in `is_beating()`. The kernel answers heartbeats from a dedicated thread, so a busy kernel still beats. Sustained silence on a local connection therefore means the kernel process has exited. conkernelclient declares death after three consecutive unanswered echoes rather than one, because a single missed echo can be scheduler noise. From either signal, conkernelclient builds the same dead `status` message rustygate broadcasts and passes it to `route()`. Death therefore reaches `route()` in one shape on both transports.

Waiters also end at client close. jupyasyncclient's `aclose` and conkernelclient's `stop_channels` fail both dicts the same way, with a `RuntimeError` naming the close rather than `DeadKernelError`. jupyasyncclient's `_reconnect` fails them the same way when its retry ceiling expires.

## .shell() and .control()

The remaining requests all follow `reply()`'s pattern. On shell: `kernel_info_request`, `complete_request`, `inspect_request`, `is_complete_request`, `history_request`. On control: `interrupt_request`, `release_request`, `shutdown_request`, `create_subshell_request`, `debug_request`. Each request's whole result is its reply. None produces outputs. None is worth sending without wanting the answer.

One sender implements the pattern for all ten requests. It builds the named message with `session.msg`, files the future in `replies`, sends the message on the given channel through the transport's `send(msg, channel)`, and returns the awaitable. `.shell(name, **content)` and `.control(name, **content)` are one-line sugar naming the channel. `reply()` becomes sugar for `.shell('execute_request', ...)` with the execute-specific content. Control replies need nothing in `route()`, because a control reply resolves through `replies` exactly as a shell reply does.

The typed verbs `complete`, `inspect`, `check`, and `history` remain, written once over `.shell()` with the reply's useful fields extracted.

Comms are the one shell-channel send outside this pattern. A comm message never gets a reply, so `comm_open` and `comm_msg` build their messages and send them through `send(msg, channel)` without filing anything, returning the msg_id. Inbound comm traffic follows the general routing. A comm message parented to a `run()` is collected by that run. Any other comm message reaches `on_jmsg`.

## on_output

A streaming consumer wants each message as it arrives rather than a list at the end. clikernel's `stream.py` emits an event per output. `run(code, on_output=cb)` serves it. The entry grows a fifth slot, and `_run_msg` passes each message to `cb` as it appends it. The collected contract is unchanged.

We considered a queue in place of the callback and rejected it. A queue reader needs its own termination signal. `run()` would have to hand out the queue before completion. The death path would have to push exceptions into queues as well as futures. The callback has none of those obligations.

## stdin

When running code calls `input()`, the kernel sends an `input_request` on the stdin channel and blocks until the client answers. That request is the channel's entire inbound traffic. The reply direction belongs to the client, so `route()` never sees an `input_reply`.

The full exchange, for a cell that prompts:

```python
# 1. A cell runs, stdin permitted:
kc.execute("name = input('who? ')", msg_id='cell1.ab12', allow_stdin=True)

# 2. The kernel reaches input(), blocks, and sends on stdin:
{'channel': 'stdin', 'msg_type': 'input_request',
 'parent_header': {'msg_id': 'cell1.ab12'}, 'content': {'prompt': 'who? ', 'password': False}}

# 3. route() stores the header and hands the message to on_jmsg, where the app shows the prompt:
if msg.get('channel') == 'stdin':
    self._last_stdin_req = msg['header']
    return self.on_jmsg(msg)

# 4. The app calls kc.input('Jeremy'), which answers on stdin, parented to the stored request:
def input(self, string):
    parent, self._last_stdin_req = self._last_stdin_req, None
    self.send(self.session.msg('input_reply', dict(value=string), parent=parent), 'stdin')

# 5. The kernel's input() unblocks, returns 'Jeremy', and the cell runs on to idle as usual.
```

The stdin branch sits after the death check and before the `runs` match, so an `input_request` reaches `on_jmsg` even when its execute is a `run()`. `run()` therefore collects every parented shell, control, and iopub message, and no stdin message. One handler answers prompts for `execute()` and `run()` alike.

The protocol assumes one outstanding request, because the kernel blocks until the answer arrives. `_last_stdin_req` is a single slot for the same reason. `input()` clears the slot as it answers, so each prompt takes exactly one answer.

With every branch in place, the whole of `route()` is:

```python
def route(self, msg):
    if msg['msg_type'] == 'status' and msg['content'].get('execution_state') == 'dead': return self._kernel_died(msg)
    if msg.get('channel') == 'stdin':
        self._last_stdin_req = msg['header']
        return self.on_jmsg(msg)
    parent = msg.get('parent_header', {}).get('msg_id')
    if (r := self.runs.get(parent)) is not None: return self._run_msg(parent, r, msg)
    if msg['channel'] in ('shell', 'control'):
        if (fut := self.replies.pop(parent, None)) is not None and not fut.done(): return fut.set_result(msg)
    return self.on_jmsg(msg)
```

## on_jmsg and pulling

`route()` passes every message not matched by `replies` or `runs` to `on_jmsg`: cell outputs and statuses, stdin requests, `cells` traffic, fire-and-forget replies, the dead status. An app that sets no callback drops them all, and dropping is the correct default, because nothing then accumulates unread. A handler must tolerate message types it does not use. Solveit's `process_jmsg` already does, because an `execute_reply` matches none of its branches and falls through.

`on_jmsg` may be sync or async. When the handler returns an awaitable, `_pump` and `_recv_loop` await it before reading the next message, which preserves arrival order. A handler therefore stays a cheap dispatcher and hands heavy work onward. Solveit's and ipyai's handlers already have that form.

Some consumers pull rather than accept calls. ipymini's protocol tests await the next iopub message directly. The pull form is a consumer of the push form, and `JmsgQueues` is that consumer. It holds one `asyncio.Queue` per configured channel, sets itself as the client's `on_jmsg` (and as `kc.jmsgq`, so helpers such as `iopub_drain` can find it), dispatches each message to its channel's queue through a merge map, and serves `get(channel, timeout=)`:

```python
qs = JmsgQueues(kc, merge=dict(iopub='jmsg', stdin='jmsg'))
msg = await qs.get('jmsg')
```

Neither push nor pull is primary, and no client attaches `JmsgQueues` by default. `route()` knows only `on_jmsg`. Today's client-level accessors (`get_iopub_msg`, `get_shell_msg`, `get_stdin_msg`, `get_control_msg`, `get_jmsg`, `jmsg_for`) leave the clients and become `JmsgQueues` methods. `jmsg_flush` has no replacement, because with no `JmsgQueues` attached nothing accumulates to flush. conkernelclient's `iopub_drain` remains, rewritten over an attached `JmsgQueues`, because ipymini's protocol tests drain requests they already sent without a `run()`.

A slow handler delays all inbound traffic, because `_pump` and `_recv_loop` read nothing while a handler runs. That delay is a choice about where slowness surfaces rather than a defect. Buffering exists at every layer below. An iopub PUB socket at its high-water mark silently drops messages. TCP makes a sender wait. An in-process queue grows without bound. rustygate itself keeps draining the kernel's zmq sockets whatever the client does. A slow websocket client therefore fills TCP and rustygate's buffers and never the kernel's PUB queue.

## What this deletes

The `Run` class and its `claim` machinery. `execute(reply=True)` and the register-at-send branches in both transports. The per-client `reply()` patches, replaced by the one implementation in jupywire. The `fail_pending` cascade, which no consumer passes. The stale-reply swallow set, because a late reply now finds no entry and flows to `on_jmsg`. The client-level `get_*` accessors and `jmsg_for`, which move to `JmsgQueues`, and `jmsg_flush`, which nothing needs. App-side `is_alive` polling loops for death detection. Solveit's awaited cell replies and `exec_hold`'s reply future, both of which existed only to surface kernel death.
