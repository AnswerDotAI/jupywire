"""Conformance tests for `jupywire.route`: the DESIGN.md contract, driven by an in-memory fake transport.

Every client of `RouterOps` gets exactly these behaviors; the live-kernel notebooks in
jupyasyncclient and conkernelclient demonstrate the same contracts over real transports.
"""
import asyncio, contextvars
from queue import Empty

import pytest

from jupywire.route import RouterOps, JmsgQueues, DeadKernelError
from jupywire.session import Session


class FakeClient(RouterOps):
    "A `RouterOps` inheritor whose `execute` and `send` record; tests deliver kernel traffic via `route`."
    def __init__(self):
        self.session = Session(key=b'test')
        self.sent, self.sent_msgs = [], []
        self._init_router()

    def execute(self, code, msg_id=None, **kw):
        msg_id = msg_id or self.new_msg_id()
        self.sent.append((msg_id, code, kw))
        return msg_id

    def send(self, msg, channel): self.sent_msgs.append((channel, msg))


def _msg(mtype, parent, channel='iopub', **content):
    return dict(msg_type=mtype, channel=channel, parent_header=dict(msg_id=parent), content=content,
        header=dict(msg_id=f'k.{mtype}.{parent}', msg_type=mtype))

def _reply(parent, status='ok', **content):
    return _msg('execute_reply', parent, channel='shell', status=status, execution_count=1, **content)

def _outs(parent, *texts):
    "One execution's iopub traffic: busy, a stream per text, idle."
    yield _msg('status', parent, execution_state='busy')
    for t in texts: yield _msg('stream', parent, name='stdout', text=t)
    yield _msg('status', parent, execution_state='idle')

def _watch(kc):
    "Attach a recording `on_jmsg`; returns the record list."
    seen = []
    kc.on_jmsg = seen.append
    return seen

async def alist(gen):
    "Collect an async generator into a list."
    return [m async for m in gen]


async def test_reply_lifecycle():
    "Replies correlate by parent id; iopub and late replies remain application traffic."
    kc = FakeClient()
    seen = _watch(kc)
    w1, w2 = kc.reply('a'), kc.reply('b', msg_id='cell1.aa11')
    first = kc.sent[0][0]
    assert kc.sent[1][0] == 'cell1.aa11'

    kc.route(_msg('status', first, execution_state='busy'))
    kc.route(_msg('stream', first, name='stdout', text='out'))
    kc.route(_reply('cell1.aa11'))
    kc.route(_reply(first))
    r1, r2 = await w1, await w2
    assert r1['parent_header']['msg_id'] == first
    assert r2['parent_header']['msg_id'] == 'cell1.aa11'
    assert [m['msg_type'] for m in seen] == ['status', 'stream']
    assert not kc.replies

    w = kc.reply('slow', timeout=0.01)
    slow = kc.sent[-1][0]
    with pytest.raises(TimeoutError): await w
    assert not kc.replies
    kc.route(_reply(slow))
    assert seen[-1]['msg_type'] == 'execute_reply'

    kc.default_timeout = 0.01
    with pytest.raises(TimeoutError): await kc.reply('slow again')
    assert not kc.replies


async def test_run_completion_boundary():
    "A run needs reply plus idle; once complete, stragglers return to application routing."
    kc = FakeClient()
    seen = _watch(kc)
    for i,idle_first in enumerate((True, False)):
        mid = f'c.2.{i}'
        gen = kc.run('x', msg_id=mid)
        outs = list(_outs(mid, 'hi'))
        for msg in (outs + [_reply(mid)]) if idle_first else ([_reply(mid)] + outs): kc.route(msg)
        msgs = await alist(gen)
        assert len(msgs) == 4
        assert {m['msg_type'] for m in msgs} == {'status', 'stream', 'execute_reply'}
        assert not kc.runs

    late = _msg('stream', mid, name='stdout', text='from a background thread')
    kc.route(late)
    assert seen == [late]


async def test_runs_concurrent_and_isolated():
    kc = FakeClient()
    seen = _watch(kc)
    g1, g2 = kc.run('a', msg_id='c1.x'), kc.run('b', msg_id='c2.y')
    for m in _outs('c2.y', 'two'): kc.route(m)
    for m in _outs('c1.x', 'one'): kc.route(m)
    kc.route(_msg('stream', 'foreign', name='stdout', text='NOT MINE'))
    kc.route(_reply('c1.x'))
    kc.route(_reply('c2.y'))
    m1, m2 = await alist(g1), await alist(g2)
    assert [m['content']['text'] for m in m1 if m['msg_type'] == 'stream'] == ['one']
    assert [m['content']['text'] for m in m2 if m['msg_type'] == 'stream'] == ['two']
    assert seen[0]['content']['text'] == 'NOT MINE'
    assert not kc.runs


async def test_run_streaming_and_cleanup():
    "Runs send at call time, yield messages as they arrive, and an abandoned stream cleans up."
    kc = FakeClient()
    seen = _watch(kc)
    gen = kc.run('x', msg_id='c.4')
    assert kc.sent[-1][0] == 'c.4' and 'c.4' in kc.runs   # filed and sent before any consumption
    assert kc.sent[-1][2]['allow_stdin'] is False
    kc.route(_msg('status', 'c.4', execution_state='busy'))
    assert (await anext(gen))['msg_type'] == 'status'
    await gen.aclose()   # abandoned mid-run: the entry is popped, stragglers flow to `on_jmsg`
    assert not kc.runs
    kc.route(_reply('c.4'))
    assert seen[-1]['msg_type'] == 'execute_reply'

    task = asyncio.create_task(kc.exec_outs('x', msg_id='c.5'))
    await asyncio.sleep(0)
    for m in _outs('c.5', 'hello'): kc.route(m)
    kc.route(_reply('c.5'))
    assert await task == [dict(output_type='stream', name='stdout', text='hello')]

    with pytest.raises(TimeoutError): await alist(kc.run('slow', timeout=0.01))
    assert not kc.runs


async def test_waiter_failure_cleanup():
    "Kernel death, transport loss, and failed sends wake callers and leave no filed state."
    kc = FakeClient()
    seen = _watch(kc)
    w = kc.reply('a')
    task = asyncio.create_task(alist(kc.run('b')))
    dead = dict(msg_type='status', channel='iopub', parent_header={}, header={}, content=dict(execution_state='dead'))
    kc.route(dead)
    with pytest.raises(DeadKernelError): await w
    with pytest.raises(DeadKernelError): await task
    assert seen[-1] is dead
    assert not kc.replies and not kc.runs

    lost = FakeClient()
    w = lost.reply('a')
    task = asyncio.create_task(alist(lost.run('b')))
    lost.fail_waiters(RuntimeError('socket closed'))
    with pytest.raises(RuntimeError, match='socket closed'): await w
    with pytest.raises(RuntimeError, match='socket closed'): await task
    assert not lost.replies and not lost.runs

    class Broken(FakeClient):
        def execute(self, code, **kw): raise OSError('socket closed')
        def send(self, msg, channel): raise OSError('socket closed')
    broken = Broken()
    with pytest.raises(OSError): broken.reply('x')
    with pytest.raises(OSError): broken.run('x')
    with pytest.raises(OSError): broken.request('kernel_info_request')
    assert not broken.replies and not broken.runs


async def test_run_stdin_callback_lifecycle():
    "A callback answers in its caller context; a failed callback interrupts before raising."
    kc = FakeClient()
    seen, prompts = _watch(kc), []
    caller = contextvars.ContextVar('caller', default='transport')
    async def on_stdin(msg):
        await asyncio.sleep(0)
        prompts.append((caller.get(), msg))
        return 'Jeremy'
    caller.set('run')
    task = asyncio.create_task(alist(kc.run('x', msg_id='c.6', on_stdin=on_stdin)))
    caller.set('transport')
    assert kc.sent[-1][2]['allow_stdin'] is True
    req = _msg('input_request', 'c.6', channel='stdin', prompt='who? ')
    assert kc.route(req) is None
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert prompts == [('run', req)] and not seen
    ch, m = kc.sent_msgs[-1]
    assert (ch, m['content']) == ('stdin', dict(value='Jeremy'))
    assert m['parent_header']['msg_id'] == req['header']['msg_id']
    for m2 in _outs('c.6'): kc.route(m2)
    kc.route(_reply('c.6'))
    assert not any(m2['msg_type'] == 'input_request' for m2 in await task)

    def fail(msg): raise ValueError('input refused')
    failed = FakeClient()
    task = asyncio.create_task(alist(failed.run('x', msg_id='c.7', on_stdin=fail)))
    failed.route(_msg('input_request', 'c.7', channel='stdin', prompt='secret? '))
    await asyncio.sleep(0)
    channel,msg = failed.sent_msgs[-1]
    assert (channel, msg['header']['msg_type']) == ('control', 'interrupt_request')
    failed.route(_msg('interrupt_reply', msg['header']['msg_id'], channel='control', status='ok'))
    for m in _outs('c.7'): failed.route(m)
    failed.route(_reply('c.7', status='error'))
    with pytest.raises(ValueError, match='input refused'): await task


async def test_unmatched_message_consumers():
    "The push callback preserves order; `JmsgQueues` adapts the same route to pulling."
    kc = FakeClient()
    order = []
    async def handler(m):
        await asyncio.sleep(0)
        order.append(m['content']['text'])
    kc.on_jmsg = handler
    for t in ('a', 'b', 'c'):
        r = kc.route(_msg('stream', 'x', name='stdout', text=t))
        if r is not None: await r
    assert order == ['a', 'b', 'c']

    pulled = FakeClient()
    qs = JmsgQueues(pulled, queues=('shell', 'jmsg'), merge=dict(iopub='jmsg', stdin='jmsg'))
    assert pulled.on_jmsg is qs and pulled.jmsgq is qs
    pulled.route(_msg('stream', 'x', name='stdout', text='hi'))
    pulled.route(_msg('input_request', 'x', channel='stdin', prompt='? '))
    pulled.route(_reply('unclaimed'))
    assert (await qs.get('jmsg', timeout=1))['msg_type'] == 'stream'
    assert (await qs.jmsg_for('input_request', timeout=1))['msg_type'] == 'input_request'
    assert (await qs.get_shell_msg(timeout=1))['msg_type'] == 'execute_reply'
    pulled.route(dict(_msg('stream', 'x'), channel='unknown'))
    with pytest.raises(Empty): await qs.get('jmsg', timeout=0.01)


async def test_request_and_typed_verbs():
    kc = FakeClient()
    def answer(name, channel='shell', **content):
        ch, m = kc.sent_msgs[-1]
        assert (ch, m['header']['msg_type']) == (channel, name)
        kc.route(dict(_msg(name.replace('_request', '_reply'), m['header']['msg_id'], channel=channel), content=content))
    w = kc.control('interrupt_request')
    answer('interrupt_request', 'control', status='ok')
    assert (await w)['content']['status'] == 'ok'

    task = asyncio.ensure_future(kc.complete('imp'))
    await asyncio.sleep(0)
    answer('complete_request', matches=['import'], cursor_start=0)
    assert await task == (['import'], 0)

    task = asyncio.ensure_future(kc.check('for i in x:'))
    await asyncio.sleep(0)
    answer('is_complete_request', status='incomplete', indent='    ')
    assert await task == ('incomplete', '    ')

    task = asyncio.ensure_future(kc.inspect('no_such'))
    await asyncio.sleep(0)
    answer('inspect_request', found=False)
    assert await task == ''

    task = asyncio.ensure_future(kc.history())
    await asyncio.sleep(0)
    assert kc.sent_msgs[-1][1]['content']['session'] == 0   # range access defaults filled in
    answer('history_request', history=[[0, 1, 'x=1']])
    assert (await task)['content']['history'] == [[0, 1, 'x=1']]

    w = kc.request('kernel_info_request', channel='control', msg_id='fixed-id-1')
    ch, m = kc.sent_msgs[-1]
    assert m['header']['msg_id'] == 'fixed-id-1'
    kc.route(dict(_msg('kernel_info_reply', 'fixed-id-1', channel='control'), content=dict(status='ok')))
    assert (await w)['parent_header']['msg_id'] == 'fixed-id-1'


def test_comms_are_fire_and_forget():
    kc = FakeClient()
    mid = kc.comm_msg('co1', dict(x=1), buffers=[b'raw'])
    ch, msg = kc.sent_msgs[-1]
    assert mid == msg['header']['msg_id']
    assert (ch, msg['header']['msg_type'], msg['content'], msg['buffers']) == (
        'shell', 'comm_msg', dict(comm_id='co1', data=dict(x=1)), [b'raw'])

    kc.comm_open('tgt', 'co1', dict(y=2))
    assert kc.sent_msgs[-1][1]['content'] == dict(comm_id='co1', target_name='tgt', data=dict(y=2))
    assert not kc.replies
