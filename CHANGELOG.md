<!-- do not remove -->

## 0.1.5

### New Features

- Rework RouterOps: reply()/run()/`on_jmsg` replace three-tier queues and Run; add JmsgQueues, generic request seam, kernel-death handling ([#8](https://github.com/AnswerDotAI/jupywire/issues/8))
- Add jupywire.route with three-tier RouterOps message routing and Run, plus `eval_expr`/`user_exprs` in ops ([#7](https://github.com/AnswerDotAI/jupywire/issues/7))

### Bugs Squashed

- Fix eval error handling to report ename/evalue when kernel reply has no traceback ([#6](https://github.com/AnswerDotAI/jupywire/issues/6))


## 0.1.4

### New Features

- Replace priority subshell routing with kernmini priority:1 execute metadata; ipyfuncs default priority on, eval/retr off ([#5](https://github.com/AnswerDotAI/jupywire/issues/5))


## 0.1.3

### New Features

- Extract more ops ([#4](https://github.com/AnswerDotAI/jupywire/issues/4))


## 0.1.2

### New Features

- Make xpush/xenv sync fire-and-forget via new abstract execute() seam, alongside the awaited reply() seam ([#3](https://github.com/AnswerDotAI/jupywire/issues/3))


## 0.1.1

### New Features

- Inline `pack_frames`/`unpack_frames` in session.py ([#2](https://github.com/AnswerDotAI/jupywire/issues/2))


## 0.1.0

### New Features

- init release ([#1](https://github.com/AnswerDotAI/jupywire/issues/1))
