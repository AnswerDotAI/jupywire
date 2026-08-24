"""Conformance tests for `jupywire.route`: the DESIGN.md contract, driven by an in-memory fake transport.

Every client of `RouterOps` gets exactly these behaviors; the live-kernel notebooks in
jupyasyncclient and conkernelclient demonstrate the same contracts over real transports.
"""
import asyncio
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


async def test_reply_resolution():
    kc = FakeClient()
    w1, w2 = kc.reply('a'), kc.reply('b', msg_id='cell1.aa11')
    assert kc.sent[1][0] == 'cell1.aa11'   # the caller's msg_id goes on the wire
    kc.route(_reply('cell1.aa11'))
    kc.route(_reply(kc.sent[0][0]))
    r1, r2 = await w1, await w2
    assert r1['parent_header']['msg_id'] == kc.sent[0][0]
    assert r2['parent_header']['msg_id'] == 'cell1.aa11'
    assert not kc.replies


async def test_reply_ignores_own_iopub():
    "A reply()'s execute also causes iopub traffic; only the shell reply may resolve the future."
    kc = FakeClient()
    seen = _watch(kc)
    w = kc.reply('a', msg_id='c.1')
    kc.route(_msg('status', 'c.1', execution_state='busy'))
    kc.route(_msg('stream', 'c.1', name='stdout', text='out'))
    kc.route(_reply('c.1'))
    assert (await w)['msg_type'] == 'execute_reply'
    assert [m['msg_type'] for m in seen] == ['status', 'stream']


async def test_reply_timeout_and_late_arrival():
    kc = FakeClient()
    seen = _watch(kc)
    w = kc.reply('slow', timeout=0.01)
    with pytest.raises(TimeoutError): await w
    assert not kc.replies                                # the timeout popped the entry
    kc.route(_reply(kc.sent[0][0]))
    assert seen[-1]['msg_type'] == 'execute_reply'       # the late reply reaches on_jmsg
    kc.default_timeout = 0.01
    with pytest.raises(TimeoutError): await kc.reply('slow again')   # a client-level default timeout applies when none is passed
    assert not kc.replies


async def test_run_collects_both_terminator_orders():
    kc = FakeClient()
    for idle_first in (True, False):
        task = asyncio.ensure_future(kc.run('x', msg_id='c.2'))
        await asyncio.sleep(0)
        outs = list(_outs('c.2', 'hi'))
        for m in (outs + [_reply('c.2')]) if idle_first else ([_reply('c.2')] + outs): kc.route(m)
        msgs = await asyncio.wait_for(task, 5)
        assert len(msgs) == 4
        assert {m['msg_type'] for m in msgs} == {'status', 'stream', 'execute_reply'}
        assert not kc.runs


async def test_run_straggler_reaches_on_jmsg():
    kc = FakeClient()
    seen = _watch(kc)
    task = asyncio.ensure_future(kc.run('x', msg_id='c.3'))
    await asyncio.sleep(0)
    for m in _outs('c.3'): kc.route(m)
    kc.route(_reply('c.3'))
    await task
    late = _msg('stream', 'c.3', name='stdout', text='from a background thread')
    kc.route(late)
    assert seen == [late]


async def test_runs_concurrent_and_isolated():
    kc = FakeClient()
    seen = _watch(kc)
    t1 = asyncio.ensure_future(kc.run('a', msg_id='c1.x'))
    t2 = asyncio.ensure_future(kc.run('b', msg_id='c2.y'))
    await asyncio.sleep(0)
    for m in _outs('c2.y', 'two'): kc.route(m)
    for m in _outs('c1.x', 'one'): kc.route(m)
    kc.route(_msg('stream', 'foreign', name='stdout', text='NOT MINE'))
    kc.route(_reply('c1.x'))
    kc.route(_reply('c2.y'))
    m1, m2 = await asyncio.gather(t1, t2)
    assert [m['content']['text'] for m in m1 if m['msg_type'] == 'stream'] == ['one']
    assert [m['content']['text'] for m in m2 if m['msg_type'] == 'stream'] == ['two']
    assert seen[0]['content']['text'] == 'NOT MINE'
    assert not kc.runs


async def test_run_lifecycle():
    kc = FakeClient()
    coro = kc.run('never')
    assert not kc.sent and not kc.runs      # unawaited: nothing sent, nothing filed
    coro.close()
    streamed = []
    task = asyncio.ensure_future(kc.run('x', on_output=streamed.append, msg_id='c.4'))
    await asyncio.sleep(0)
    for m in _outs('c.4', 'live'): kc.route(m)
    assert [m['msg_type'] for m in streamed] == ['status', 'stream', 'status']   # streamed before completion
    kc.route(_reply('c.4'))
    await task
    with pytest.raises(TimeoutError): await kc.run('slow', timeout=0.01)
    assert not kc.runs                      # the timeout cleaned its entry


async def test_exec_outs():
    kc = FakeClient()
    task = asyncio.ensure_future(kc.exec_outs('x', msg_id='c.5'))
    await asyncio.sleep(0)
    for m in _outs('c.5', 'hello'): kc.route(m)
    kc.route(_reply('c.5'))
    assert await task == [dict(output_type='stream', name='stdout', text='hello')]


async def test_kernel_death_fails_everything():
    kc = FakeClient()
    seen = _watch(kc)
    w = kc.reply('a')
    task = asyncio.ensure_future(kc.run('b'))
    await asyncio.sleep(0)
    dead = dict(msg_type='status', channel='iopub', parent_header={}, header={}, content=dict(execution_state='dead'))
    kc.route(dead)
    with pytest.raises(DeadKernelError): await w
    with pytest.raises(DeadKernelError): await asyncio.wait_for(task, 5)
    assert seen[-1] is dead                 # the status still reaches the app
    assert not kc.replies and not kc.runs
    kc.fail_waiters(DeadKernelError('socket closed'))   # the transport-loss path needs no message


async def test_stdin_slot_and_input():
    kc = FakeClient()
    seen = _watch(kc)
    task = asyncio.ensure_future(kc.run('x', msg_id='c.6', allow_stdin=True))
    await asyncio.sleep(0)
    req = _msg('input_request', 'c.6', channel='stdin', prompt='who? ')
    kc.route(req)
    assert seen == [req]                    # stdin bypasses the run
    kc.input('Jeremy')
    ch, m = kc.sent_msgs[-1]
    assert (ch, m['content']) == ('stdin', dict(value='Jeremy'))
    assert m['parent_header']['msg_id'] == req['header']['msg_id']
    assert kc._last_stdin_req is None       # answer-once
    for m2 in _outs('c.6'): kc.route(m2)
    kc.route(_reply('c.6'))
    assert not any(m2['msg_type'] == 'input_request' for m2 in await task)


async def test_async_on_jmsg_preserves_order():
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

    mid = kc.comm_msg('co1', dict(x=1), buffers=[b'raw'])
    ch, m = kc.sent_msgs[-1]
    assert mid == m['header']['msg_id']
    assert (ch, m['header']['msg_type'], m['content'], m['buffers']) == ('shell', 'comm_msg', dict(comm_id='co1', data=dict(x=1)), [b'raw'])
    kc.comm_open('tgt', 'co1', dict(y=2))
    assert kc.sent_msgs[-1][1]['content'] == dict(comm_id='co1', target_name='tgt', data=dict(y=2))

    w = kc.request('kernel_info_request', channel='control', msg_id='fixed-id-1')
    ch, m = kc.sent_msgs[-1]
    assert m['header']['msg_id'] == 'fixed-id-1'
    kc.route(dict(_msg('kernel_info_reply', 'fixed-id-1', channel='control'), content=dict(status='ok')))
    assert (await w)['parent_header']['msg_id'] == 'fixed-id-1'


async def test_jmsg_queues():
    kc = FakeClient()
    qs = JmsgQueues(kc, queues=('shell', 'jmsg'), merge=dict(iopub='jmsg', stdin='jmsg'))
    assert kc.on_jmsg is qs and kc.jmsgq is qs
    kc.route(_msg('stream', 'x', name='stdout', text='hi'))
    kc.route(_msg('input_request', 'x', channel='stdin', prompt='? '))
    kc.route(_reply('unclaimed'))
    assert (await qs.get('jmsg', timeout=1))['msg_type'] == 'stream'
    assert (await qs.jmsg_for('input_request', timeout=1))['msg_type'] == 'input_request'
    assert (await qs.get_shell_msg(timeout=1))['msg_type'] == 'execute_reply'
    kc.route(dict(_msg('stream', 'x'), channel='unknown'))   # a channel with no queue: dropped
    with pytest.raises(Empty): await qs.get('jmsg', timeout=0.01)


async def test_send_failure_leaves_no_entry():
    class Broken(FakeClient):
        def execute(self, code, **kw): raise OSError('socket closed')
        def send(self, msg, channel): raise OSError('socket closed')
    kc = Broken()
    with pytest.raises(OSError): kc.reply('x')
    assert not kc.replies
    with pytest.raises(OSError): await kc.run('x')
    assert not kc.runs
    with pytest.raises(OSError): kc.request('kernel_info_request')
    assert not kc.replies
