"""Client-side call conventions for a Jupyter kernel: `eval`, `ipy`, and friends over shared client routing seams.

`EvalOps` is a mixin for kernel clients (conkernelclient's `ConKernelClient`, jupyasyncclient's
`JupyAsyncKernelClient`): the inheritor supplies `reply(code, user_expressions=, timeout=,
**kw)` awaiting its transport's `execute_reply` message, and `execute(code, **kw)`
sending without awaiting anything. It gets the whole calling surface: `eval` (call a kernel-side
function by name, result reconstructed by repr), `ipy` (`get_ipython()` methods), the generated
`ipyfuncs` service methods, `retr`, and the sync `xpush`/`xenv`. Setting names in a kernel has no
useful reply, so those two are sync and fire-and-forget; ordering still holds, because a transport
delivers requests in send order.
`sidecar_=` routes through the kernel's persistent named sidecar subshell. The `ipyfuncs` service
methods default it on, since each is a small out-of-band call made on the user's behalf. `xpush`
and `retr` also default to that serial lane; ordinary `eval` defaults to the main shell.
`_pre_ipy` is a liveness hook (default no-op).
The module also carries the message-dict helpers shared by both clients' consumers: `parent_id`,
`iopub_msgs`, and the `output_types` set.
"""

import asyncio
from ast import literal_eval
from fastcore.utils import rtoken_hex, nested_idx
from fastcore.ansi import strip_ansi
from fastcore.nbio import preferred_out

class EvalException(Exception): pass

_prims = str, int, float, complex, bool, tuple, list, dict, set, frozenset

def try_eval(s, typ:str|None=None):
    "Like `literal_eval`, but wraps in dynamic class named `typ` if succeeds, and returns `s` if fails"
    try:
        res = literal_eval(s)
        if typ and isinstance(res, _prims): res = type(typ, (type(res),), {})(res)
        return res
    except: return s


class EvalError(Exception):
    "An `eval_expr` expression raised in the kernel"

def parse_expr(s):
    "The `literal_eval` of `s` when its form allows, else `s` unchanged"
    try: return literal_eval(s)
    except Exception: return s


output_types = {'stream', 'execute_result', 'display_data', 'error'}
def parent_id(msg):
    "The `msg_id` of the request this `msg` responds to, or None"
    return nested_idx(msg, "parent_header", "msg_id") or None

def iopub_msgs(msgs, msg_type=None):
    "Filter iopub `msgs` by `msg_type` - a single type, or a collection of types (all messages if None)"
    if msg_type is None: return msgs
    types = {msg_type} if isinstance(msg_type, str) else msg_type
    return [m for m in msgs if m['msg_type'] in types]


class EvalOps:
    "Kernel-client calling conventions over the inheritor's `reply` and `execute`: see the module docstring."

    def reply(self, code, user_expressions=None, timeout=None, **kw):
        "The awaited transport seam: run `code`, returning an awaitable of the `execute_reply` message."
        raise NotImplementedError

    def execute(self, code, **kw):
        "The fire-and-forget transport seam: send `code`, returning its msg_id, without awaiting a reply."
        raise NotImplementedError

    def _pre_ipy(self): pass   # liveness hook: transports may raise their dead-kernel error here

    async def eval(self, func:str, *args, timeout_=60, literal_=True, sidecar_=False, call_=True, msg_id_=None, **kw):
        "Result of running `func(*args, **kw)`"
        vname = f'__{rtoken_hex(4)}'
        if call_:
            code = f'''import asyncio
{vname} = {func}(*{args!r}, **{kw!r})
if asyncio.iscoroutine({vname}): {vname} = await {vname}
'''
        else: code = f'{vname} = {func}'
        exprs = dict(__res=vname, __typ=f"type({vname}).__name__", __del=f"globals().pop('{vname}', None)")
        kw2 = dict(user_expressions=exprs, timeout=timeout_, store_history=False)
        if sidecar_: kw2['subshell_id'] = 'sidecar'
        if msg_id_ is not None: kw2['msg_id'] = msg_id_
        try: cts = (await self.reply(code, **kw2))['content']
        except TimeoutError: return 'timeout'
        except TypeError as e: raise EvalException(f"Eval failed: {e}")  # e.g. kernel not running
        if cts['status']!='ok':
            if tb := cts.get('traceback'): return strip_ansi('\n'.join(tb))
            detail = ' '.join(filter(None, (cts.get('ename'), cts.get('evalue'))))
            return f"{cts['status']}: {detail}" if detail else cts['status']
        typ = nested_idx(cts, 'user_expressions', '__typ', 'data', 'text/plain')
        if typ: typ = typ.strip("'")
        res = nested_idx(cts, 'user_expressions', '__res', 'data')
        if not res: return res
        res = preferred_out(res, html1st=False)[1]
        try: return try_eval(res, typ) if literal_ else res
        except Exception as e: return str(e)

    async def ipy(self, meth, *args, sidecar_=True, timeout_=5, **kwargs):
        if not hasattr(self, '_ipylock'): self._ipylock = asyncio.Lock()
        self._pre_ipy()
        async with self._ipylock: return await self.eval('get_ipython().'+meth, sidecar_=sidecar_, timeout_=timeout_, *args, **kwargs)

    def xpush(self, sidecar_=True, **kwargs):
        "Bind `kwargs` as names in the kernel's user namespace"
        return self.execute(f'get_ipython().push({kwargs!r})', subshell_id='sidecar' if sidecar_ else None)

    async def retr(self, nm:str, sidecar_=True):
        "Retrieve a single variable value"
        return await self.eval(nm, call_=False, sidecar_=sidecar_, timeout_=60)

    async def user_exprs(self, exprs:dict, code:str='', timeout=10, **kw):
        "Run `code` and evaluate each of the `exprs` expressions in the same round trip; returns the reply content (statuses and mimebundles unparsed)"
        return (await self.reply(code, user_expressions=exprs, timeout=timeout, store_history=False, **kw))['content']

    async def eval_expr(self, expr:str, code:str='', timeout=10, **kw):
        "Evaluate `expr` in the kernel (optionally after running `code`) and return its value: parsed via `literal_eval` when its repr allows, else the repr string. Raises `EvalError` if the kernel raises"
        cts = await self.user_exprs({'_': expr}, code, timeout=timeout, **kw)
        if cts['status'] != 'ok': raise EvalError(f"{cts.get('ename')}: {cts.get('evalue')}")
        r = cts['user_expressions']['_']
        if r.get('status') != 'ok': raise EvalError(f"{r.get('ename')}: {r.get('evalue')}")
        return parse_expr(r['data']['text/plain'])

    def xenv(self, **kw):
        "Put all of `kw` in os.environ"
        code = 'import os as __os\n'
        code += '\n'.join(f'__os.environ[{k!r}]={str(v)!r}' for k,v in kw.items())
        self.execute(code)


def _mk_ipy(meth):
    async def f(self, *args, **kwargs): return await self.ipy(meth, *args, **kwargs)
    f.__name__ = meth
    setattr(EvalOps, meth, f)

_ipy_funcs = ['user_items', 'get_vars', 'eval_exprs', 'get_schemas', 'publish', 'ranked_complete', 'sig_help']
for o in _ipy_funcs: _mk_ipy(o)
