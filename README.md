# agui-stream-doctor

Offline, dependency-free checker and state replayer for **recorded AG-UI 1.0 event streams** — because the AG-UI JSON Schema validates one event at a time, and the protocol itself says the interesting cross-event rules are left to the consumer.

Upstream: https://github.com/ag-ui-protocol/ag-ui

Why you need it now: AG-UI 1.0 has 31 event types across 8 families and a published JSON Schema, but nothing in the protocol or any SDK ships a *language-agnostic* tool that reads a recorded stream and answers "would a consumer survive this?". Every SDK does its own partial runtime enforcement (TypeScript zod, Python pydantic, .NET), so the same recorded stream gets accepted or silently mangled depending on which client you point at it.

## What the upstream is, and what it does not do

AG-UI is the Agent-User Interaction Protocol: an HTTP/SSE event protocol that streams agent runs — text messages, tool calls, state snapshots/deltas, activity, subagents — to a frontend. It publishes a complete JSON Schema at `docs/spec/1.0/schema.json`.

Three limits are **stated by the specification itself**, not inferred:

1. Run lifecycle is all that is mandatory — `docs/spec/1.0/events/index.mdx`, line 23: *"Only the run lifecycle is mandatory for a producer — every other family is a feature it emits when it has something to say with it."* So a schema-valid stream can still contain a `TEXT_MESSAGE_CONTENT` for a message nobody ever started.
   <https://github.com/ag-ui-protocol/ag-ui/blob/main/docs/spec/1.0/events/index.mdx#L23>
2. Concatenated tool-call arguments are never validated — `docs/spec/1.0/schema.mdx`, line 132: the `delta` fragments concatenate into the argument text, *"conventionally a JSON document, though the protocol does not validate it"*.
   <https://github.com/ag-ui-protocol/ag-ui/blob/main/docs/spec/1.0/schema.mdx#L132>
3. A `STATE_DELTA` can be structurally perfect and still not apply — `schema.json` → `$defs.StateDeltaEvent`: *"Structural validity here does not mean the patch applies: a well-formed operation may point at a path that does not exist, which RFC 6902 leaves to the applier."*
   <https://github.com/ag-ui-protocol/ag-ui/blob/main/docs/spec/1.0/schema.json>
4. Real-world evidence that this bites: open issue [#3391](https://github.com/ag-ui-protocol/ag-ui/issues/3391) — *"enforceEvents accepts malformed RUN_FINISHED outcomes"*; issue [#3368](https://github.com/ag-ui-protocol/ag-ui/issues/3368) — *"AGUI.Server reopens a closed text message when text resumes after a tool call"*; issue [#3212](https://github.com/ag-ui-protocol/ag-ui/issues/3212) — *"MCPMiddleware runs tool calls left over from maxIterations on the next run"*.

## What this tool does

`agui_stream_doctor.py` reads a recorded stream (JSONL, or SSE `data:` frames) from disk and:

- checks every event against the 31-type `EventType` enum and the required fields of its definition;
- checks the run lifecycle: events outside a run, nested runs, terminal events with no open run, a run left open at end of stream;
- checks message and tool-call pairing: content/end without start, twice-opened ids, args after end, unclosed messages/tool calls/steps;
- **concatenates `TOOL_CALL_ARGS` and validates the result as JSON** — the thing the spec explicitly does not do;
- checks `RUN_FINISHED.outcome` shape (the #3391 failure mode);
- **replays** `STATE_SNAPSHOT` / `STATE_DELTA` with an RFC 6902 applier and reports which patch operation fails to apply.

It does **not**: run an agent, speak to a network, implement the full RFC 6902 spec (only `add/remove/replace/move/copy/test` with standard JSON Pointer escaping), judge semantic correctness of messages, or validate against the JSON Schema's every constraint (numeric bounds, `unevaluatedProperties`, and union types are approximated by the required-field table above).

## Install / run

Python 3.8+ standard library only. No install step, no dependencies.

```bash
python3 agui_stream_doctor.py examples/in/clean.jsonl --json --replay
```

Human output on the intentionally broken sample:

```bash
$ python3 agui_stream_doctor.py examples/in/broken.jsonl
ERROR [E-RUN-ORDER] #0 TEXT_MESSAGE_CONTENT: TEXT_MESSAGE_CONTENT outside a run ...
ERROR [E-TOOL-UNOPENED] #2 TOOL_CALL_ARGS: arguments for tool call 'call-orphan' that was never started
ERROR [E-TOOL-ARGS-NOT-JSON] #5 TOOL_CALL_END: concatenated arguments for 'call-2' are not JSON (...)
ERROR [E-STATE-DELTA-NO-BASE] #6 STATE_DELTA: STATE_DELTA before any STATE_SNAPSHOT ...
ERROR [E-STATE-PATCH-FAILED] #8 STATE_DELTA: op[0] (replace) cannot apply: key 'missing' missing
ERROR [E-RUN-OUTCOME] #11 RUN_FINISHED: outcome.type is missing or not a string ...
WARNING [E-TEXT-UNCLOSED] #9 TEXT_MESSAGE_START: message 'msg-2' never closed
12 event(s), 8 error(s), 1 warning(s) -> PROBLEMS
```

`--json` output (always parseable; see `examples/out/`):

```json
{
  "tool": "agui-stream-doctor",
  "version": "1.0.0",
  "spec": "AG-UI 1.0",
  "events": 12,
  "ok": false,
  "counts": {"error": 8, "warning": 1},
  "findings": [
    {"code": "E-RUN-ORDER", "severity": "error", "event_index": 0,
     "event_type": "TEXT_MESSAGE_CONTENT", "detail": "..."}
  ],
  "parse_errors": [],
  "final_state": {"a": 1}
}
```

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | stream clean at the selected `--fail-on` level |
| `1` | findings at or above `--fail-on` (default `error`) |
| `2` | usage error, unreadable file, empty input, or no line could be parsed |

`--fail-on error|warning|never` and `--format auto|jsonl|sse` are the only knobs. `--replay` adds `final_state` (JSON Pointer/patch application is always run internally when `--replay` is given).

## Limitations

- The required-field table is derived from `docs/spec/1.0/schema.json` at 2026-11-15 but is a hand-maintained subset; new upstream fields are not auto-picked-up.
- Type checking is `isinstance`-based (JSON types), not full JSON Schema; `minimum`/`maxLength`/`unevaluatedProperties` are not enforced.
- `TEXT_MESSAGE_CHUNK` / `TOOL_CALL_CHUNK` are treated as shorthand and only loosely tracked; the prose spec resolves which message a field-less chunk continues, and this tool does not.
- SSE parsing takes `data:` lines only (no multi-line event concatenation, no `event:`/`id:` fields).
- No test corpus of real provider streams ships here; `examples/in/*` are hand-built minimal witnesses.

## 与同类项目的差异

Searched before writing (2026-11-15, real commands):

- `gh search repos "AG-UI event stream validator" --limit=20` → **no results**
- `gh search repos "ag-ui protocol lint" --limit=20` → **no results**
- `gh search repos "ag-ui" --limit=20 --sort=stars` → top hits are the protocol repo itself, CopilotKit (the frontend stack, 38.7k), `thesysdev/openui` (a different generative-UI standard), and app/framework repos. No offline stream checker.

So there is no mature (>500 stars) equivalent to displace. What exists instead is *per-SDK runtime enforcement* inside the protocol repo itself, which is exactly the fragmentation this tool bypasses: it needs no SDK, no language runtime, and no network.

## 本轮创新

**B 档. 新在哪:** 把 AG-UI 从「逐事件 JSON Schema 校验」推进到「已录制流的跨事件生命周期 + RFC 6902 可应用性判定」，并且是语言无关的离线实现。协议文档自己写了三处「schema 不管这个」（`index.mdx#L23` 只有 run lifecycle 是强制的、`schema.mdx#L132` 参数拼接不做校验、`schema.json` StateDeltaEvent 明说结构合法不等于 patch 能应用），但没有任何工具去测这三处；本工具用可复现的最小 witness 把它们变成退出码。凭什么是第一个填坑的：`gh search repos` 对两类关键词均为 0 命中，上游 31 种事件只有各 SDK 各自的部分运行时检查，没有离线、跨语言、跨事件的可判定实现。

## License

MIT — see `LICENSE`.
