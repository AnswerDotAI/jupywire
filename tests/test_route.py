"""Conformance tests for the three-tier router and `Run`, driven by an in-memory fake transport.

Every client of `RouterOps` gets exactly these behaviors; the live-kernel notebooks in
jupyasyncclient and conkernelclient demonstrate the same contracts over real transports.
"""
import asyncio
from queue import Empty

import pytest

from jupywire.route import RouterOps, Run, DeadKernelError
from jupywire.session import Session


class FakeClient(RouterOps):
    "A `RouterOps` inheritor whose `execute` records sends; tests deliver kernel traffic via `route`."
    def __init__(self, **kw):
        self.session = Session(key=b'test')
        self.sent, self.inputs, self.alive = [], [], True
        self._init_router(**kw)

    def execute(self, code, msg_id=None, **kw):
        msg_id = msg_id or self.new_msg_id()
        self.sent.append((msg_id, code, kw))
        return msg_id

    async def is_alive(self): return self.alive
    def input(self, string): self.inputs.append(string)


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


async def test_reply_waiters_concurrent():
    kc = FakeClient()
    f1, f2 = kc._register_reply('m1'), kc._register_reply('m2')
    kc.route(_reply('m2'))
    kc.route(_reply('m1'))
    r1 = await kc.await_reply(f1, 'm1', timeout=1)
    r2 = await kc.await_reply(f2, 'm2', timeout=1)
    assert (r1['parent_header']['msg_id'], r2['parent_header']['msg_id']) == ('m1', 'm2')


async def test_stale_reply_dropped():
    kc = FakeClient()
    fut = kc._register_reply('slow')
    with pytest.raises(TimeoutError): await kc.await_reply(fut, 'slow', timeout=0.01)
    kc.route(_reply('slow'))   # late arrival: swallowed, not queued
    with pytest.raises(Empty): await kc.get_msg('shell', timeout=0.01)


async def test_fail_pending_cascade():
    kc = FakeClient()
    bad = kc._register_reply('bad', fail_pending=True)
    other = kc._register_reply('other')
    kc.route(_reply('bad', status='error', ename='ValueError', evalue='boom'))
    assert (await kc.await_reply(bad, 'bad', timeout=1))['content']['status'] == 'error'
    with pytest.raises(RuntimeError, match='ValueError'): await kc.await_reply(other, 'other', timeout=1)


async def test_channel_queues_and_merge():
    kc = FakeClient(queues=('shell', 'control', 'jmsg'), merge=dict(iopub='jmsg', stdin='jmsg'))
    kc.route(_msg('stream', 'x', name='stdout', text='hi'))
    kc.route(_msg('input_request', 'x', channel='stdin', prompt='? '))
    assert (await kc.get_msg('jmsg', timeout=1))['msg_type'] == 'stream'
    assert (await kc.get_msg('jmsg', timeout=1))['msg_type'] == 'input_request'
    assert kc._last_stdin_req is not None   # remembered for `input`
    kc.route(dict(_msg('stream', 'x'), channel='unknown'))   # unmapped channel with no queue: dropped
    with pytest.raises(Empty): await kc.get_msg('jmsg', timeout=0.01)


async def test_concurrent_runs_isolated():
    "The #15 contract: several runs in flight each collect only their own traffic, and the raw tier still sees the rest."
    kc = FakeClient()
    r1, r2 = Run(kc, 'a'), Run(kc, 'b')
    for m in _outs(r2.msg_id, 'two'): kc.route(m)
    for m in _outs(r1.msg_id, 'one'): kc.route(m)
    kc.route(_msg('stream', 'foreign', name='stdout', text='NOT MINE'))
    kc.route(_reply(r1.msg_id))
    kc.route(_reply(r2.msg_id))
    o1, o2 = await asyncio.wait_for(asyncio.gather(r1.collect(), r2.collect()), 5)
    assert [o['text'] for o in o1] == ['one'] and [o['text'] for o in o2] == ['two']
    assert r1.status == r2.status == 'ok'
    assert (await kc.get_msg('iopub', timeout=1))['parent_header']['msg_id'] == 'foreign'
    assert not kc._claims   # both runs released their claims


async def test_run_claim_precedes_send():
    "Traffic delivered synchronously at send time still reaches the run: the claim exists before `execute` returns."
    class EagerClient(FakeClient):
        def execute(self, code, msg_id=None, **kw):
            for m in _outs(msg_id, 'early'): self.route(m)
            self.route(_reply(msg_id))
            return super().execute(code, msg_id=msg_id, **kw)
    outs = await EagerClient().run('x')
    assert [o['text'] for o in outs] == ['early']


async def test_run_stdin_and_comm():
    kc = FakeClient()
    comms = []
    async def on_stdin(prompt, password): return f'ans:{prompt}'
    run = Run(kc, 'x', on_stdin=on_stdin, on_comm=lambda mt, c: comms.append(mt))
    kc.route(_msg('input_request', run.msg_id, channel='stdin', prompt='who'))
    kc.route(_msg('comm_msg', run.msg_id, comm_id='c1'))
    for m in _outs(run.msg_id): kc.route(m)
    kc.route(_reply(run.msg_id))
    await asyncio.wait_for(run.collect(), 5)
    assert kc.inputs == ['ans:who'] and comms == ['comm_msg']
    assert kc.sent[0][2]['allow_stdin'] is True


async def test_run_never_iterated_leaks_nothing():
    kc = FakeClient()
    run = Run(kc, 'x')
    assert run.msg_id in kc._claims
    run.close()
    assert not kc._claims
    class Broken(FakeClient):
        def execute(self, code, **kw): raise OSError('socket closed')
    kc2 = Broken()
    with pytest.raises(OSError): Run(kc2, 'x')
    assert not kc2._claims   # a failed send unregisters its claim


async def test_fail_all_ends_runs_and_waiters():
    kc = FakeClient()
    fut = kc._register_reply('m1')
    run = Run(kc, 'x')
    task = asyncio.ensure_future(run.collect())
    await asyncio.sleep(0)
    kc.fail_all(DeadKernelError('gone'))
    with pytest.raises(DeadKernelError): await asyncio.wait_for(task, 5)
    with pytest.raises(DeadKernelError): await kc.await_reply(fut, 'm1', timeout=1)


async def test_dead_kernel_detected_on_silence():
    kc = FakeClient()
    kc.alive = False
    run = Run(kc, 'x')
    with pytest.raises(DeadKernelError): await asyncio.wait_for(run.collect(), 5)
    assert not kc._claims


async def test_claim_and_waiter_are_exclusive():
    kc = FakeClient()
    run = Run(kc, 'x')
    with pytest.raises(AssertionError): kc._register_reply(run.msg_id)
    fut = kc._register_reply('w1')
    with pytest.raises(AssertionError): kc.claim('w1')
    run.close()


class VerbClient(FakeClient):
    "A `request` seam over the fake transport: tests answer from `self.answers` by request name."
    def __init__(self, **answers):
        super().__init__()
        self.answers = answers

    def request(self, name, content=None, channel='shell', reply=True, timeout=None, buffers=None, **kw):
        msg = self.session.msg(name, content or {})
        self.sent.append((msg['msg_id'], name, content, buffers))
        if not reply: return msg['msg_id']
        fut = self._register_reply(msg['msg_id'], channel)
        self.route(dict(_msg(name.replace('_request', '_reply'), msg['msg_id'], channel=channel), content=self.answers[name]))
        return self.await_reply(fut, msg['msg_id'], timeout=timeout, channel=channel)


async def test_typed_verbs_over_request_seam():
    kc = VerbClient(complete_request=dict(matches=['import'], cursor_start=0), inspect_request=dict(found=True, data={'text/plain': 'sig'}),
        is_complete_request=dict(status='incomplete', indent='    '), history_request=dict(history=[[0, 1, 'x=1']]))
    assert await kc.complete('imp') == (['import'], 0)
    assert await kc.inspect('print') == 'sig'
    assert await kc.check('for i in x:') == ('incomplete', '    ')
    assert (await kc.history())['content']['history'] == [[0, 1, 'x=1']]
    assert kc.sent[-1][2]['session'] == 0   # range access defaults filled in


async def test_inspect_not_found_and_comm_send():
    kc = VerbClient(inspect_request=dict(found=False))
    assert await kc.inspect('no_such_name') == ''
    kc.comm_msg('c1', dict(x=1), buffers=[b'raw'])
    assert kc.sent[-1][1:] == ('comm_msg', dict(comm_id='c1', data=dict(x=1)), [b'raw'])
