# jupywire

Minimal Jupyter protocol commons: wire-format sessions (message construction, HMAC signing, frame serialize/deserialize) and kernel-client call conventions, shared by kernmini, jupygate, jupyasyncclient, and conkernelclient. Depends on fastcore alone.

## Development

```bash
pip install -e .[dev]
```

## Versioning

Version lives in `jupywire/__init__.py` as `__version__`.
Bump it with:

```bash
ship-bump --part 2   # patch
ship-bump --part 1   # minor
ship-bump --part 0   # major
```

## Release

1) Ensure your GitHub issues are labeled (`bug`, `enhancement`, `breaking`).
2) Run:

```bash
ship-release
```
