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

## The named sidecar

Reentrant service calls should not overtake the main shell queue or require the running cell to make that queue concurrent. `EvalOps` tags them with `subshell_id='sidecar'`; kernmini creates a missing named subshell when the first tagged request arrives. Its small `ipyfuncs` service methods and variable operations (`xpush`, `get_vars`, `eval_exprs`, and `retr`) use that serial lane by default. Ordinary `eval` remains on the serial main shell unless explicitly routed.

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

Tool calls need a third pattern. Most tools run as silent executes, where ipyai's `KernelBridge` sends the tool call with `reply()` and reads the result back through `user_expressions`. The `py` tool cannot, because its result is whatever the code printed or displayed, and those arrive as iopub messages. The same need appears in clikernel, teleprint, and test helpers. Each runs one piece of code and wants its outputs back, and the interactive consumers want each output as it arrives, not a list at the end.

`run(code)` sends at call time, exactly as `reply()` does, and returns an async generator yielding every message parented to its execute, up to and including the `execute_reply` and the idle status:

```python
def run(self, code, on_stdin=None, timeout=None, msg_id=None, **kw):
    mid = msg_id or self.new_msg_id()
    end = None if timeout is None else asyncio.get_running_loop().time() + timeout
    r = self.runs[mid] = dict2obj(mid=mid, q=asyncio.Queue(), got_reply=False, got_idle=False,
        on_stdin=on_stdin, stdin_tasks=set(), error=None, stdin_context=contextvars.copy_context())
    try: self.execute(code, msg_id=mid, allow_stdin=on_stdin is not None, **kw)
    except BaseException:
        self.runs.pop(mid, None)
        raise
    return self._run_gen(r, end)

async def _run_gen(self, r, end):
    try:
        while True:
            async with asyncio.timeout_at(end): m = await r.q.get()
            if m is _END:
                if r.error is not None: raise r.error
                return
            yield m
    finally:
        self.runs.pop(r.mid, None)
        for task in r.stdin_tasks: task.cancel()
```

Each run entry names its state: the message queue `q`, `got_reply`, `got_idle`, the stdin hook, the active `stdin_tasks`, any stdin callback `error`, and the caller's `stdin_context`. `dict2obj` keeps reads legible (`r.q`, `r.error`); mutations retain item assignment because its attribute form is read-only. Entry filing and the send both happen in the plain `def`, before the generator exists, so no reply can slip past and wire order is call order. Concurrent calls have independent entries and collect independently. stdin is disabled unless the caller supplies `on_stdin`, binding permission to the handler that can actually service it, and stdin is a routing event, never yielded as output. Completion requires both the shell reply and the idle status because channels do not share an ordering guarantee.

The generator's `finally` is the one cleanup path for every exit: completion, timeout (the deadline is fixed at send time), an error raised through the sentinel, or a consumer abandoning the stream. An abandoned run's entry is popped, so its remaining traffic flows to `on_jmsg` like any other unmatched message; a consumer that breaks out early closes the generator deterministically with `aclosing`. A generator never iterated at all leaves its entry until the kernel finishes the cell, at which point `_run_msg` pops it: a fire-and-forget execute whose buffering cleans itself up.

`route()` grows one branch, pulled out as a helper:

```python
def route(self, msg):
    parent = msg.get('parent_header', {}).get('msg_id')
    if (r := self.runs.get(parent)) is not None: return self._run_msg(parent, r, msg)
    if msg['channel'] in ('shell', 'control'):
        if (fut := self.replies.pop(parent, None)) is not None and not fut.done(): return fut.set_result(msg)
    self.on_jmsg(msg)

def _run_msg(self, parent, r, msg):
    r.q.put_nowait(msg)
    mt = msg['msg_type']
    if mt == 'execute_reply': r['got_reply'] = True
    if mt == 'status' and msg['content'].get('execution_state') == 'idle': r['got_idle'] = True
    if r.got_reply and r.got_idle:
        self.runs.pop(parent)
        r.q.put_nowait(_END)
```

`route()` pops the entry as it resolves, so a message parented to a finished run flows to `on_jmsg` like any other unmatched message. Output printed by a background thread after idle reaches the app rather than a dead entry.

Many callers of `run()` want rendered outputs rather than raw messages. `exec_outs(code)` is the listified form, keeping only the output messages, converted to nbformat form by fastcore.nbio's `msg2out`:

```python
async def exec_outs(self, code, **kw):
    return [msg2out(m) async for m in self.run(code, **kw) if m['msg_type'] in OUTPUT_MSGS]
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
    for r in self.runs.values():
        if r.error is None: r['error'] = exc
        r.q.put_nowait(_END)
        for task in r.stdin_tasks: task.cancel()
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

## The stdin hook

A streaming consumer needs no hook: it iterates the generator and renders inside its own loop body, awaiting freely. clikernel's `stream.py` and ipyai's `run_cell` work this way, and errors in their rendering propagate to their callers like any other exception.

`on_stdin` remains a hook because it is not merely a notification. It receives an `input_request` parented to the run and returns that prompt's answer, either directly or through an awaitable. jupywire sends the resulting `input_reply`, correctly parented to the request. A caller describes the interaction in one straight-line callback; it does not manage a second queue, a continuation loop, or prompt lifecycle state.

## stdin

When running code calls `input()`, the kernel sends an `input_request` on the stdin channel and blocks until the client answers. That request is the channel's entire inbound traffic. The reply direction belongs to the client, so `route()` never sees an `input_reply`.

`run()` owns stdin for executions it owns. Its `on_stdin` hook receives the complete `input_request`. Supplying the hook sets `allow_stdin=True`; omitting it sets `allow_stdin=False`, so a noninteractive collected run gets `StdinNotImplementedError` rather than hanging.

```python
async def answer(req):
    return await ui.prompt(req['content']['prompt'])

msgs = await kc.run("name = input('who? ')", on_stdin=answer)
```

The callback may wait as long as the user does, but `route()` must not. The same receive loop has to keep routing control replies and kernel-death signals while the callback is suspended. The stdin branch therefore starts one task for the exchange and returns immediately. That task runs in a copy of the context captured by `run()`, not the transport reader's context; request-scoped `ContextVar` values therefore remain attached to the callback that registered them. `_answer_stdin` awaits the callback when necessary, then calls `input(value, request)` with the complete request so concurrent runs cannot cross-parent their replies.

An stdin request parented to a run with a hook goes only to that hook. An unmatched request—for example one caused by fire-and-forget `execute()`—goes to `on_jmsg`, preserving the application-level cell path. stdin messages are routing events, not execution outputs, so `run()` never includes them in its returned message list.

Each run records its active stdin tasks. Completion, cancellation, client close, and kernel death cancel them. If a callback raises, `_answer_stdin` records the error and sends `interrupt_request`; the kernel is not left blocked forever in `input()`. The run raises that error only after its execute has reached reply and idle, so a caller may safely execute again immediately. This is also why queues are the wrong primitive here: they require separate readers, termination sentinels, exception propagation, and continuation state for a request/reply exchange that is already exactly represented by a callable.

`input(value, request)` remains public for unmatched application-level prompts and low-level clients. With an explicit request it parents the reply to that request. Without one it answers the most recent unmatched request remembered by `route()`.

With every branch in place, the whole of `route()` is:

```python
def route(self, msg):
    if msg['msg_type'] == 'status' and msg['content'].get('execution_state') == 'dead': return self._kernel_died(msg)
    parent = msg.get('parent_header', {}).get('msg_id')
    if msg.get('channel') == 'stdin':
        self._last_stdin_req = msg.get('header')
        if (r := self.runs.get(parent)) is not None and r.on_stdin is not None:
            task = asyncio.create_task(self._answer_stdin(r, msg), context=r.stdin_context.copy())
            r.stdin_tasks.add(task)
            task.add_done_callback(r.stdin_tasks.discard)
            return
        return self._jmsg(msg)
    if (r := self.runs.get(parent)) is not None: return self._run_msg(parent, r, msg)
    if msg['channel'] in ('shell', 'control'):
        if (fut := self.replies.pop(parent, None)) is not None and not fut.done(): return fut.set_result(msg)
    return self._jmsg(msg)

async def _answer_stdin(self, r, msg):
    try:
        value = r.on_stdin(msg)
        if inspect.isawaitable(value): value = await value
        self.input(value, msg)
    except Exception as e:
        r['error'] = e
        try: await self.control('interrupt_request')
        except Exception:
            r.q.put_nowait(_END)
```

## on_jmsg and pulling

`route()` passes every message not matched by `replies`, `runs`, or a run's `on_stdin` hook to `on_jmsg`: cell outputs and statuses, unmatched stdin requests, `cells` traffic, fire-and-forget replies, and the dead status. An app that sets no callback drops them all, and dropping is the correct default, because nothing then accumulates unread. A handler must tolerate message types it does not use. Solveit's `process_jmsg` already does, because an `execute_reply` matches none of its branches and falls through.

`on_jmsg` may be sync or async. When the handler returns an awaitable, `_pump` and `_recv_loop` await it before reading the next message, which preserves arrival order. A handler therefore stays a cheap dispatcher and hands heavy work onward. `on_stdin` is deliberately different: `route()` schedules its exchange independently so waiting for human input never stops the receive loop. Solveit's and ipyai's application handlers already have the cheap-dispatcher form.

Some consumers pull rather than accept calls. ipymini's protocol tests await the next iopub message directly. The pull form is a consumer of the push form, and `JmsgQueues` is that consumer. It holds one `asyncio.Queue` per configured channel, sets itself as the client's `on_jmsg` (and as `kc.jmsgq`, so helpers such as `iopub_drain` can find it), dispatches each message to its channel's queue through a merge map, and serves `get(channel, timeout=)`:

```python
qs = JmsgQueues(kc, merge=dict(iopub='jmsg', stdin='jmsg'))
msg = await qs.get('jmsg')
```

Neither push nor pull is primary, and no client attaches `JmsgQueues` by default. `route()` knows only `on_jmsg`. Today's client-level accessors (`get_iopub_msg`, `get_shell_msg`, `get_stdin_msg`, `get_control_msg`, `get_jmsg`, `jmsg_for`) leave the clients and become `JmsgQueues` methods. `jmsg_flush` has no replacement, because with no `JmsgQueues` attached nothing accumulates to flush. conkernelclient's `iopub_drain` remains, rewritten over an attached `JmsgQueues`, because ipymini's protocol tests drain requests they already sent without a `run()`.

A slow handler delays all inbound traffic, because `_pump` and `_recv_loop` read nothing while a handler runs. That delay is a choice about where slowness surfaces rather than a defect. Buffering exists at every layer below. An iopub PUB socket at its high-water mark silently drops messages. TCP makes a sender wait. An in-process queue grows without bound. rustygate itself keeps draining the kernel's zmq sockets whatever the client does. A slow websocket client therefore fills TCP and rustygate's buffers and never the kernel's PUB queue.

## What this deletes

The `Run` class and its `claim` machinery. `execute(reply=True)` and the register-at-send branches in both transports. The per-client `reply()` patches, replaced by the one implementation in jupywire. The `fail_pending` cascade, which no consumer passes. The stale-reply swallow set, because a late reply now finds no entry and flows to `on_jmsg`. The client-level `get_*` accessors and `jmsg_for`, which move to `JmsgQueues`, and `jmsg_flush`, which nothing needs. App-side `is_alive` polling loops for death detection. Solveit's awaited cell replies and `exec_hold`'s reply future, both of which existed only to surface kernel death.
