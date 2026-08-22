"""Transport-independent message routing for Jupyter kernel clients, plus `Run`.

`RouterOps` is a mixin for kernel clients (conkernelclient's `ConKernelClient`, jupyasyncclient's
`JupyAsyncKernelClient`): the transport feeds every inbound message to `route`, and the mixin
delivers each one to exactly one of three tiers. A reply whose parent msg_id has a waiting
`reply=True` future resolves it (tier one). A message parented to a claimed execute goes to that
execute's queue, which is how `Run` streams one execution's traffic while others are in flight
(tier two). Everything else queues on its channel for the `get_msg` accessors, the raw view that
frontends, monitors, and protocol tests read (tier three). The tiers have fixed precedence; the
only per-transport routing configuration is the queue map given to `_init_router`.

The inheritor supplies `execute(code, msg_id=, ...)` sending without awaiting (`Run` pre-generates
the id so its claim is registered before anything is on the wire), an async `is_alive`, a
`session` whose `msg_id` generates fresh ids, and pumps calling `route`. `input` answers the most
recent `input_request`, which `route` remembers. `request` sends a named protocol request and
returns an awaitable of its reply; the typed verbs (`complete`, `inspect`, `check`, `history`,
`comm_msg`) are written once over that seam, so every transport gets the same calling surface.
"""

import asyncio
from queue import Empty
from fastcore.nbio import msg2out

OUTPUT_MSGS = ('stream', 'display_data', 'execute_result', 'error')
COMM_MSGS = ('comm_open', 'comm_msg', 'comm_close')

class DeadKernelError(RuntimeError): pass


class RouterOps:
    "Three-tier routing over a transport that calls `route(msg)`: reply waiters, claimed executes, channel queues."
    def _init_router(self,
        queues=('shell', 'control', 'iopub', 'stdin'), # Channel queue names for unclaimed traffic
        merge=None, # Channel-to-queue aliases, e.g. `dict(iopub='jmsg')`; unmapped channels use their own name
        reply_channels=('shell', 'control'), # Channels whose requests can register reply waiters
    ):
        self._queues = {k: asyncio.Queue() for k in queues}
        self._merge = merge or {}
        self._reply_waiters = {k: {} for k in reply_channels}
        self._stale_replies = {k: set() for k in reply_channels}
        self._claims = {}   # msg_id -> queue of every message parented to that claimed execute
        self._last_stdin_req = None

    def route(self, msg):
        "Deliver one inbound message to the first tier that owns it; unroutable messages are dropped."
        channel = msg.get('channel') or 'shell'
        msg.setdefault('msg_id', msg.get('header', {}).get('msg_id'))
        msg.setdefault('msg_type', msg.get('header', {}).get('msg_type'))
        msg.setdefault('buffers', [])
        if channel == 'stdin' and msg['msg_type'] == 'input_request': self._last_stdin_req = msg.get('header')
        parent_msg_id = msg.get('parent_header', {}).get('msg_id')
        if channel in self._reply_waiters and parent_msg_id:
            pend = self._reply_waiters[channel].pop(parent_msg_id, None)
            if pend:
                fut, fail_pending = pend
                if not fut.done(): fut.set_result(msg)
                cts = msg.get('content', {})
                if fail_pending and cts.get('status') in ('error', 'aborted'):
                    exc = RuntimeError(f"Kernel error aborted: {cts.get('ename')}: {cts.get('evalue')}")
                    self.fail_pending(exc, channel, skip=parent_msg_id)
                return
            if parent_msg_id in self._stale_replies[channel]:
                self._stale_replies[channel].discard(parent_msg_id)
                return
        if (q := self._claims.get(parent_msg_id)) is not None: return q.put_nowait(msg)
        if (q := self._queues.get(self._merge.get(channel, channel))) is not None: q.put_nowait(msg)

    def fail_pending(self, exc, channel='shell', skip=None):
        "Fail pending reply waiters on `channel` (except `skip`), e.g. when an error aborts the kernel's queue."
        for mid, (fut, _) in list(self._reply_waiters[channel].items()):
            if mid != skip and not fut.done(): fut.set_exception(exc)

    def fail_all(self, exc):
        "Fail every reply waiter and claimed execute: transport loss, kernel death, close."
        for ch in self._reply_waiters: self.fail_pending(exc, ch)
        for q in self._claims.values(): q.put_nowait(exc)

    def claim(self, msg_id):
        "Register a queue receiving every message parented to `msg_id`; release with `unclaim`."
        assert msg_id not in self._claims, f'{msg_id} already claimed'
        assert not any(msg_id in w for w in self._reply_waiters.values()), f'{msg_id} has a reply waiter, which would swallow its reply'
        q = self._claims[msg_id] = asyncio.Queue()
        return q

    def unclaim(self, msg_id): self._claims.pop(msg_id, None)

    def new_msg_id(self): return self.session.msg_id

    def run(self, code, on_stdin=None, on_comm=None, **kw):
        "Execute `code`, returning a `Run` over this execution's nbformat-shaped outputs."
        return Run(self, code, on_stdin=on_stdin, on_comm=on_comm, **kw)

    def request(self, name, content=None, channel='shell', reply=True, timeout=None, buffers=None, **kw):
        "The generic-request seam the typed verbs below use: send the named request, returning an awaitable of its reply (or the msg_id when `reply=False`). Supplied by the transport."
        raise NotImplementedError

    async def complete(self, code, cursor_pos=None, timeout=15):
        "Completion `(matches, replace_start)` for `code` at `cursor_pos` (end when None)"
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        c = (await self.request('complete_request', dict(code=code, cursor_pos=cursor_pos), timeout=timeout))['content']
        return c.get('matches', []), c.get('cursor_start', cursor_pos)

    async def inspect(self, code, cursor_pos=None, detail_level=0, timeout=15):
        "Inspection text for `code` at `cursor_pos` ('' when nothing found)"
        cursor_pos = len(code) if cursor_pos is None else cursor_pos
        c = (await self.request('inspect_request', dict(code=code, cursor_pos=cursor_pos, detail_level=detail_level), timeout=timeout))['content']
        return c.get('data', {}).get('text/plain', '') if c.get('found') else ''

    async def check(self, code, timeout=15):
        "('complete'|'incomplete'|'invalid'|'unknown', indent) for `code` as a cell"
        c = (await self.request('is_complete_request', dict(code=code), timeout=timeout))['content']
        return c.get('status', 'unknown'), c.get('indent', '')

    async def history(self, raw=True, output=False, hist_access_type='range', timeout=15, **kw):
        "The `history_reply` message; range access defaults to the whole current session"
        if hist_access_type == 'range': kw = dict(session=0, start=0) | kw
        return await self.request('history_request', dict(raw=raw, output=output, hist_access_type=hist_access_type, **kw), timeout=timeout)

    def comm_msg(self, comm_id, data=None, buffers=None):
        "Send a `comm_msg` to the kernel, fire-and-forget: comms never get replies"
        return self.request('comm_msg', dict(comm_id=comm_id, data=data or {}), reply=False, buffers=buffers)

    def _register_reply(self, msg_id, channel='shell', fail_pending=False):
        "A future `route` resolves with the reply parented to `msg_id`; await it via `await_reply`."
        assert msg_id not in self._claims, f'{msg_id} is claimed: a waiter would swallow its reply'
        fut = asyncio.get_running_loop().create_future()
        self._reply_waiters[channel][msg_id] = (fut, fail_pending)
        return fut

    async def await_reply(self, fut, msg_id, timeout=None, channel='shell'):
        "Await a registered reply future; a timeout or cancellation marks the reply stale, so its late arrival is dropped."
        try:
            async with asyncio.timeout(timeout): return await fut
        finally:
            popped = self._reply_waiters[channel].pop(msg_id, None)
            if popped and popped[0] is fut and (not fut.done() or fut.cancelled()): self._stale_replies[channel].add(msg_id)

    async def get_msg(self, channel, timeout=None):
        "Next unclaimed message on `channel`, raising `queue.Empty` on timeout: the raw view."
        try:
            async with asyncio.timeout(timeout): return await self._queues[channel].get()
        except asyncio.TimeoutError as e: raise Empty from e


class Run:
    "One execution's typed output stream: its claim is registered before the send, so no message can slip past; single-use."
    def __init__(self, kc, code, on_stdin=None, on_comm=None, **kw):
        self.kc,self.on_stdin,self.on_comm = kc,on_stdin,on_comm
        self.reply,self.status,self.execution_count = None,None,None
        self.msg_id = kc.new_msg_id()
        self.q = kc.claim(self.msg_id)
        try: kc.execute(code, msg_id=self.msg_id, allow_stdin=on_stdin is not None, **kw)
        except BaseException:
            self.close()
            raise

    def close(self): self.kc.unclaim(self.msg_id)

    async def __aiter__(self):
        done = idle = False
        try:
            while not (done and idle):
                try: msg = await asyncio.wait_for(self.q.get(), 1)
                except TimeoutError:
                    if not await self.kc.is_alive(): raise DeadKernelError('kernel died while executing')
                    continue
                if isinstance(msg, Exception): raise msg   # `fail_all` lands here
                mt,c = msg['msg_type'],msg['content']
                if mt == 'execute_reply':
                    self.reply,self.status,self.execution_count = msg,c.get('status'),c.get('execution_count')
                    done = True
                elif mt == 'input_request': self.kc.input(await self.on_stdin(c.get('prompt', ''), c.get('password', False)))
                elif mt == 'status' and c.get('execution_state') == 'idle': idle = True
                elif mt in OUTPUT_MSGS: yield msg2out(msg)
                elif mt in COMM_MSGS and self.on_comm is not None: self.on_comm(mt, c)
        finally: self.close()

    async def collect(self):
        "Drain the run, returning all outputs as a list."
        return [o async for o in self]

    def __await__(self): return self.collect().__await__()
