"""Transport-independent message handling for Jupyter kernel clients; DESIGN.md is the contract.

`RouterOps` is a mixin for kernel clients (conkernelclient's `ConKernelClient`, jupyasyncclient's
`JupyAsyncKernelClient`). The transport feeds every inbound message to `route`. A shell or control
message whose parent msg_id has a `reply()` or `request` future resolves it. A message parented to
a `run()` in flight is collected by that run; its stdin request goes to that run's `on_stdin` hook,
whose return value jupywire sends as the correctly parented `input_reply`.
Every other message goes to the app's `on_jmsg` callback, and an app that sets no callback drops
them. `reply()` sends at call time and returns an awaitable of the `execute_reply`. `run()` sends
when awaited and returns every parented shell, control, and iopub message, up to and including the
`execute_reply` and the idle status. It disables stdin unless supplied an `on_stdin` hook.
`request` sends any named protocol request, `shell` and `control` name its channel, and the typed
verbs (`complete`, `inspect`, `check`, `history`) sit on top. `input` answers an explicit
`input_request`, or the most recent unmatched request that `route` remembers. A dead-kernel status fails every waiter through
`_kernel_died`; transport-loss and close paths call `fail_waiters` directly. `JmsgQueues` is the
pull adapter: it registers itself as `on_jmsg` (and `kc.jmsgq`) and serves per-channel queues.

The inheritor supplies `execute(code, msg_id=, ...)` sending without awaiting, `send(msg, channel)`
transmitting a built message dict (including its `buffers`), a `session` building messages and
fresh ids, and a read loop feeding `route` with `channel`-stamped dicts. It awaits an awaitable
returned by `route`, while stdin callbacks run in their own tasks so the read loop remains live.
"""

import asyncio, contextvars, inspect
from queue import Empty
from fastcore.nbio import msg2out
from fastcore.utils import dict2obj

OUTPUT_MSGS = ('stream', 'display_data', 'execute_result', 'error')
COMM_MSGS = ('comm_open', 'comm_msg', 'comm_close')

class DeadKernelError(RuntimeError): pass


class RouterOps:
    "Message handling over a transport that calls `route(msg)`: `reply()`, `run()`, named requests, stdin, and death."
    def _init_router(self):
        self.replies = {}    # msg_id -> future resolved with the matching shell/control reply
        self.runs = {}       # msg_id -> run state
        self.on_jmsg = None
        self._last_stdin_req = None

    def route(self, msg):
        "Deliver one inbound message; `on_jmsg` may return an awaitable, while stdin callbacks run independently."
        msg.setdefault('msg_id', msg.get('header', {}).get('msg_id'))
        msg.setdefault('msg_type', msg.get('header', {}).get('msg_type'))
        msg.setdefault('buffers', [])
        if msg['msg_type'] == 'status' and msg.get('content', {}).get('execution_state') == 'dead': return self._kernel_died(msg)
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
        if msg.get('channel') in ('shell', 'control'):   # only a real reply may resolve a future, never the request's own iopub
            if (fut := self.replies.pop(parent, None)) is not None and not fut.done(): return fut.set_result(msg)
        return self._jmsg(msg)

    def _jmsg(self, msg):
        if self.on_jmsg is not None: return self.on_jmsg(msg)

    async def _answer_stdin(self, r, msg):
        "Call this run's stdin handler and send its answer to the requesting kernel."
        try:
            value = r.on_stdin(msg)
            if inspect.isawaitable(value): value = await value
            self.input(value, msg)
        except Exception as e:
            r['error'] = e
            try: await self.control('interrupt_request')
            except Exception:
                if not r.fut.done(): r.fut.set_exception(e)

    def _run_msg(self, parent, r, msg):
        "Fold one message into its run: collect, stream, complete on reply plus idle."
        r.msgs.append(msg)
        if r.on_output is not None: r.on_output(msg)
        mt = msg['msg_type']
        if mt == 'execute_reply': r['got_reply'] = True
        if mt == 'status' and msg['content'].get('execution_state') == 'idle': r['got_idle'] = True
        if r.got_reply and r.got_idle:
            self.runs.pop(parent)
            if not r.fut.done():
                if r.error is None: r.fut.set_result(r.msgs)
                else: r.fut.set_exception(r.error)

    def fail_waiters(self, exc):
        "Fail every `reply()` future and in-flight `run()`: kernel death, transport loss, client close."
        for fut in self.replies.values():
            if not fut.done(): fut.set_exception(exc)
        for r in self.runs.values():
            if not r.fut.done(): r.fut.set_exception(exc)
            for task in r.stdin_tasks: task.cancel()
        self.replies.clear()
        self.runs.clear()

    def _kernel_died(self, msg):
        "Fail every waiter and run, then let the dead status reach the app."
        self.fail_waiters(DeadKernelError('kernel died'))
        return self._jmsg(msg)

    def new_msg_id(self): return self.session.msg_id

    def _filed(self, mid, timeout=None):
        "File a reply future for `mid` and return its awaitable; file before sending, so no reply can slip past. A None `timeout` falls back to the client's `default_timeout` attribute, when set."
        if timeout is None: timeout = getattr(self, 'default_timeout', None)
        fut = self.replies[mid] = asyncio.get_running_loop().create_future()
        async def _wait():
            try: return await asyncio.wait_for(fut, timeout)
            finally: self.replies.pop(mid, None)
        return _wait()

    def _sent_or_clean(self, mid, w, f):
        "Run the send `f`; a failed send pops the entry and closes the unawaited waiter."
        try: f()
        except BaseException:
            self.replies.pop(mid, None)
            w.close()
            raise
        return w

    def reply(self, code, timeout=None, msg_id=None, **kw):
        "Run `code`, returning an awaitable of its `execute_reply`; the send happens now."
        mid = msg_id or self.new_msg_id()
        w = self._filed(mid, timeout)
        return self._sent_or_clean(mid, w, lambda: self.execute(code, msg_id=mid, **kw))

    def request(self, name, content=None, channel='shell', timeout=None, buffers=None, msg_id=None, metadata=None, subshell_id=None):
        "Send the named protocol request on `channel`, returning an awaitable of its reply."
        msg = self.session.msg(name, content or {}, metadata=metadata)
        if buffers: msg['buffers'] = buffers
        if msg_id: msg['header']['msg_id'] = msg_id
        if subshell_id: msg['header']['subshell_id'] = subshell_id
        mid = msg['header']['msg_id']
        w = self._filed(mid, timeout)
        return self._sent_or_clean(mid, w, lambda: self.send(msg, channel))

    def shell(self, name, timeout=None, **content):
        "Send a named shell request, e.g. `shell('complete_request', code=c, cursor_pos=0)`."
        return self.request(name, content, 'shell', timeout=timeout)

    def control(self, name, timeout=None, **content):
        "Send a named control request, e.g. `control('interrupt_request')`."
        return self.request(name, content, 'control', timeout=timeout)

    async def run(self, code, on_output=None, on_stdin=None, timeout=None, msg_id=None, **kw):
        "Every message parented to this execute, up to and including its `execute_reply` and idle status; sends when awaited."
        mid = msg_id or self.new_msg_id()
        if timeout is None: timeout = getattr(self, 'default_timeout', None)
        fut = asyncio.get_running_loop().create_future()
        r = self.runs[mid] = dict2obj(msgs=[], fut=fut, got_reply=False, got_idle=False, on_output=on_output,
            on_stdin=on_stdin, stdin_tasks=set(), error=None, stdin_context=contextvars.copy_context())
        try:
            self.execute(code, msg_id=mid, allow_stdin=on_stdin is not None, **kw)
            return await asyncio.wait_for(fut, timeout)
        finally:
            self.runs.pop(mid, None)
            for task in r.stdin_tasks: task.cancel()

    async def exec_outs(self, code, **kw):
        "Just the rendered nbformat outputs of running `code`."
        return [msg2out(m) for m in await self.run(code, **kw) if m['msg_type'] in OUTPUT_MSGS]

    def input(self, string, request=None):
        "Answer an `input_request`; defaults to the most recent unmatched request."
        parent = request.get('header') if request is not None else self._last_stdin_req
        if parent == self._last_stdin_req: self._last_stdin_req = None
        self.send(self.session.msg('input_reply', dict(value=string), parent=parent), 'stdin')

    async def complete(self, code, cursor_pos=None, timeout=15):
        "Completion `(matches, replace_start)` for `code` at `cursor_pos` (end when None)"
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        c = (await self.shell('complete_request', code=code, cursor_pos=cursor_pos, timeout=timeout))['content']
        return c.get('matches', []), c.get('cursor_start', cursor_pos)

    async def inspect(self, code, cursor_pos=None, detail_level=0, timeout=15):
        "Inspection text for `code` at `cursor_pos` ('' when nothing found)"
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        c = (await self.shell('inspect_request', code=code, cursor_pos=cursor_pos, detail_level=detail_level, timeout=timeout))['content']
        return c.get('data', {}).get('text/plain', '') if c.get('found') else ''

    async def check(self, code, timeout=15):
        "('complete'|'incomplete'|'invalid'|'unknown', indent) for `code` as a cell"
        c = (await self.shell('is_complete_request', code=code, timeout=timeout))['content']
        return c.get('status', 'unknown'), c.get('indent', '')

    async def history(self, raw=True, output=False, hist_access_type='range', timeout=15, **kw):
        "The `history_reply` message; range access defaults to the whole current session"
        if hist_access_type == 'range': kw = dict(session=0, start=0) | kw
        return await self.shell('history_request', raw=raw, output=output, hist_access_type=hist_access_type, timeout=timeout, **kw)

    def _comm_send(self, name, content, buffers=None, metadata=None):
        msg = self.session.msg(name, content, metadata=metadata)
        if buffers: msg['buffers'] = buffers
        self.send(msg, 'shell')
        return msg['header']['msg_id']

    def comm_open(self, target_name, comm_id, data=None, buffers=None, metadata=None):
        "Send a `comm_open`, fire-and-forget; returns its msg_id"
        return self._comm_send('comm_open', dict(comm_id=comm_id, target_name=target_name, data=data or {}), buffers, metadata)

    def comm_msg(self, comm_id, data=None, buffers=None, metadata=None):
        "Send a `comm_msg`, fire-and-forget, comms never get replies; returns its msg_id"
        return self._comm_send('comm_msg', dict(comm_id=comm_id, data=data or {}), buffers, metadata)


class JmsgQueues:
    "Pull adapter over `on_jmsg`: one queue per name, with `merge` folding channels together; attaches itself as `kc.on_jmsg` and `kc.jmsgq`."
    def __init__(self, kc,
        queues=('shell', 'control', 'iopub', 'stdin'), # Queue names; a message whose channel maps to no queue is dropped
        merge=None, # Channel-to-queue aliases, e.g. `dict(iopub='jmsg', stdin='jmsg')`
    ):
        self.queues = {k: asyncio.Queue() for k in queues}
        self.merge = merge or {}
        kc.on_jmsg = self
        kc.jmsgq = self

    def __call__(self, msg):
        ch = msg.get('channel')
        if (q := self.queues.get(self.merge.get(ch, ch))) is not None: q.put_nowait(msg)

    async def get(self, queue, timeout=None):
        "Next message on `queue`, raising `queue.Empty` on timeout."
        try:
            async with asyncio.timeout(timeout): return await self.queues[queue].get()
        except asyncio.TimeoutError as e: raise Empty from e

    async def get_shell_msg(self, timeout=None): return await self.get('shell', timeout)
    async def get_iopub_msg(self, timeout=None): return await self.get('iopub', timeout)
    async def get_stdin_msg(self, timeout=None): return await self.get('stdin', timeout)
    async def get_control_msg(self, timeout=None): return await self.get('control', timeout)
    async def get_jmsg(self, timeout=None): return await self.get('jmsg', timeout)

    async def jmsg_for(self, *msg_types, pred=None, queue='jmsg', timeout=None):
        "The next message on `queue` matching the given types (and `pred`), raising `queue.Empty` on timeout; non-matching messages are discarded."
        try:
            async with asyncio.timeout(timeout):
                while True:
                    m = await self.queues[queue].get()
                    if (not msg_types or m['msg_type'] in msg_types) and (pred is None or pred(m)): return m
        except asyncio.TimeoutError as e: raise Empty from e
