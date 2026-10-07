"Transport-independent routing stories driven by a controllable transport; live workflows belong to the client projects."
import asyncio, contextvars
from queue import Empty

import pytest

from jupywire.route import RouterOps, JmsgQueues, DeadKernelError
from jupywire.session import Session


class FakeClient(RouterOps):
    "Record the transport boundary; deliver inbound traffic through route."
    def __init__(self):
        self.session, self.sent, self.sent_msgs, self.failure = Session(key=b'test'), [], [], None
        self._init_router()

    def execute(self, code, msg_id=None, **kw):
        if self.failure: raise self.failure
        mid = msg_id or self.new_msg_id()
        self.sent.append((mid, code, kw))
        return mid

    def send(self, msg, channel):
        if self.failure: raise self.failure
        self.sent_msgs.append((channel, msg))


def _msg(kind, parent, channel='iopub', **content):
    return dict(channel=channel, parent_header=dict(msg_id=parent), content=content,
        header=dict(msg_id=f'k.{kind}.{parent}', msg_type=kind))

def _reply(mid, **content): return _msg('execute_reply', mid, 'shell', status='ok', **content)
def _idle(mid): return _msg('status', mid, execution_state='idle')

def _finish(kc, mid):
    for msg in (_reply(mid), _idle(mid)): kc.route(msg)

async def alist(gen): return [msg async for msg in gen]


async def test_concurrent_execution_story():
    "Send immediately, stream independently, and finish only after reply AND idle in either order."
    kc, seen = FakeClient(), []
    kc.on_jmsg = seen.append
    reply = kc.reply('a')
    mid = kc.sent[-1][0]
    stream = kc.run('b', msg_id='cell.b', allow_stdin=True)
    assert [m[0] for m in kc.sent] == [mid, 'cell.b'] and set(kc.runs) == {mid, 'cell.b'}
    assert [m[2]['allow_stdin'] for m in kc.sent] == [False, True]
    waiter = asyncio.create_task(reply)
    incoming = [_msg('status', 'cell.b', execution_state='busy'), _msg('stream', mid, name='stdout', text='one'),
        _msg('stream', 'cell.b', name='stdout', text='two'), _msg('stream', 'foreign', text='not ours')]
    for msg in incoming: kc.route(msg)
    assert await anext(stream) is incoming[0]  # visible before completion, not buffered until idle
    collected = asyncio.create_task(alist(stream))
    closing = [_reply(mid), _idle('cell.b'), _idle(mid), _reply('cell.b')]
    for msg in closing[:2]: kc.route(msg)
    await asyncio.sleep(0)
    assert not waiter.done() and not collected.done() and set(kc.runs) == {mid, 'cell.b'}
    kc.route(closing[2])
    assert (await waiter)['parent_header']['msg_id'] == mid and set(kc.runs) == {'cell.b'}
    kc.route(closing[3])
    assert [m['content']['text'] for m in await collected if m['msg_type'] == 'stream'] == ['two']
    assert seen == incoming + closing and not kc.runs
    late = _msg('stream', mid, text='background output')
    kc.route(late)
    assert seen[-1] is late

    task = asyncio.create_task(kc.exec_outs('print(42)', msg_id='render'))
    await asyncio.sleep(0)
    kc.route(_msg('stream', 'render', name='stdout', text='42'))
    _finish(kc, 'render')
    assert await task == [dict(output_type='stream', name='stdout', text='42')]


async def test_failure_and_recovery_story():
    "Every exit releases routing state; late traffic and subsequent work remain usable."
    kc, seen = FakeClient(), []
    kc.on_jmsg = seen.append
    for error in (DeadKernelError('kernel died'), RuntimeError('socket closed')):
        waits = [asyncio.create_task(wait) for wait in (kc.reply('a'), alist(kc.run('b')), kc.request('kernel_info_request'))]
        await asyncio.sleep(0)
        if isinstance(error, DeadKernelError): kc.route(_msg('status', '', execution_state='dead'))
        else: kc.fail_waiters(error)
        for wait in waits:
            with pytest.raises(type(error), match=str(error)): await wait
        assert not kc.runs and not kc.replies
        recovered = kc.run('retry', msg_id='retry')
        _finish(kc, 'retry')
        assert len(await alist(recovered)) == 2
    assert sum(m['content'].get('execution_state') == 'dead' for m in seen) == 1

    kc.failure = OSError('send failed')
    for send in (lambda: kc.reply('a'), lambda: kc.run('b'), lambda: kc.request('kernel_info_request')):
        with pytest.raises(OSError, match='send failed'): send()
    assert not kc.runs and not kc.replies
    kc.failure, kc.default_timeout = None, 0
    for wait in (kc.reply('a'), alist(kc.run('b')), kc.request('kernel_info_request')):
        with pytest.raises(TimeoutError): await wait
    assert not kc.runs and not kc.replies
    kc.route(_reply(kc.sent[-1][0]))
    assert seen[-1]['msg_type'] == 'execute_reply'
    kc.default_timeout = None

    abandoned = kc.run('abandoned', msg_id='abandoned')
    kc.route(_msg('stream', 'abandoned', text='first'))
    await anext(abandoned)
    await abandoned.aclose()
    never_read = kc.run('never read', msg_id='never')
    _finish(kc, 'never')
    await never_read.aclose()
    assert not kc.runs


async def test_stdin_ownership_and_recovery_story():
    "Human input must not block routing or lose the caller context; refusal interrupts before failing."
    kc, seen, started, release = FakeClient(), [], asyncio.Event(), asyncio.Event()
    kc.on_jmsg = seen.append
    caller = contextvars.ContextVar('caller', default='transport')
    async def answer(msg):
        assert caller.get() == 'cell'
        started.set()
        await release.wait()
        return 'Jeremy'
    caller.set('cell')
    gen = kc.run('input()', msg_id='input', on_stdin=answer)
    caller.set('transport')
    req = _msg('input_request', 'input', 'stdin', prompt='who? ')
    kc.route(req)
    await asyncio.wait_for(started.wait(), 1)
    assert kc.sent[-1][2]['allow_stdin'] and seen == [req]
    stdin_task, = kc.runs['input'].stdin_tasks
    control = kc.control('interrupt_request')  # routing stays live while the input callback is suspended
    kc.route(_msg('interrupt_reply', kc.sent_msgs[-1][1]['header']['msg_id'], 'control', status='ok'))
    assert (await control)['content']['status'] == 'ok'
    release.set()
    await stdin_task
    channel, reply = kc.sent_msgs[-1]
    assert (channel, reply['content'], reply['parent_header']['msg_id']) == ('stdin', dict(value='Jeremy'), req['header']['msg_id'])
    _finish(kc, 'input')
    assert all(m['channel'] != 'stdin' for m in await alist(gen))

    def refuse(msg): raise ValueError('input refused')
    task = asyncio.create_task(alist(kc.run('again', msg_id='refused', on_stdin=refuse)))
    kc.route(_msg('input_request', 'refused', 'stdin'))
    await asyncio.sleep(0)
    channel, interrupt = kc.sent_msgs[-1]
    assert (channel, interrupt['msg_type']) == ('control', 'interrupt_request')
    kc.route(_msg('interrupt_reply', interrupt['msg_id'], 'control'))
    kc.route(_reply('refused'))
    await asyncio.sleep(0)
    assert not task.done()  # cannot retry before the kernel is idle
    kc.route(_idle('refused'))
    with pytest.raises(ValueError, match='input refused'): await task
    assert not kc.runs and not kc.replies

    started.clear()
    release.clear()
    caller.set('cell')
    gen = kc.run('cancel input', msg_id='cancel', on_stdin=answer)
    kc.route(_msg('status', 'cancel', execution_state='busy'))
    await anext(gen)
    kc.route(_msg('input_request', 'cancel', 'stdin'))
    await asyncio.wait_for(started.wait(), 1)
    stdin_task, = kc.runs['cancel'].stdin_tasks
    await gen.aclose()
    with pytest.raises(asyncio.CancelledError): await stdin_task
    assert not kc.runs


async def test_application_routing_story():
    "Push notifications are ordered; pull queues observe matched traffic without taking ownership."
    kc, seen = FakeClient(), []
    async def notify(msg):
        await asyncio.sleep(0)
        seen.append(msg)
    kc.on_jmsg = notify
    for text in ('a', 'b', 'c'): await kc.route(_msg('stream', 'foreign', text=text))
    assert [m['content']['text'] for m in seen] == ['a', 'b', 'c']
    qs = JmsgQueues(kc, queues=('shell', 'jmsg'), merge=dict(iopub='jmsg', stdin='jmsg'))
    run = kc.run('external input', msg_id='queue', allow_stdin=True)
    kc.route(_msg('stream', 'queue', text='discard during filter'))
    req = _msg('input_request', 'queue', 'stdin')
    kc.route(req)
    assert await qs.jmsg_for('input_request', timeout=1) is req
    kc.input('manual answer')
    assert kc.sent_msgs[-1][1]['parent_header']['msg_id'] == req['header']['msg_id']
    _finish(kc, 'queue')
    assert (await qs.get_shell_msg(timeout=1))['msg_type'] == 'execute_reply'
    assert (await qs.get('jmsg', timeout=1))['content']['execution_state'] == 'idle'
    assert len(await alist(run)) == 3
    kc.route(_msg('ignored', 'foreign', 'unknown'))
    with pytest.raises(Empty): await qs.get('jmsg', timeout=0)


async def test_protocol_requests_story():
    "Typed requests extract useful fields; comms send without filing a reply."
    kc = FakeClient()
    for call, name, content, expected in (
        (kc.complete('imp'), 'complete_request', dict(matches=['import'], cursor_start=0), (['import'], 0)),
        (kc.check('for i in x:'), 'is_complete_request', dict(status='incomplete', indent='    '), ('incomplete', '    ')),
        (kc.inspect('missing'), 'inspect_request', dict(found=False), ''),
    ):
        task = asyncio.create_task(call)
        await asyncio.sleep(0)
        channel, sent = kc.sent_msgs[-1]
        assert (channel, sent['msg_type']) == ('shell', name)
        kc.route(_msg(name.replace('_request', '_reply'), sent['msg_id'], 'shell', **content))
        assert await task == expected
    wait = kc.request('kernel_info_request', channel='control', msg_id='fixed')
    kc.route(_msg('status', 'fixed', execution_state='busy'))
    assert 'fixed' in kc.replies  # iopub is not a control reply
    kc.route(_msg('kernel_info_reply', 'fixed', 'control'))
    assert (await wait)['parent_header']['msg_id'] == 'fixed'
    kc.comm_open('target', 'comm')
    mid = kc.comm_msg('comm', dict(x=1), buffers=[b'raw'])
    channel, sent = kc.sent_msgs[-1]
    assert (channel, sent['msg_type'], sent['msg_id'], sent['content'], sent['buffers']) == (
        'shell', 'comm_msg', mid, dict(comm_id='comm', data=dict(x=1)), [b'raw'])
    assert not kc.replies
