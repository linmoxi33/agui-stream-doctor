#!/usr/bin/env python3
"""agui-stream-doctor - offline stream-level checker and state replayer for AG-UI 1.0.

The AG-UI 1.0 specification publishes a JSON Schema for every event, and says
two things the schema does not enforce:

  * docs/spec/1.0/events/index.mdx: "Only the run lifecycle is mandatory for a
    producer" - everything else is optional, so a stream can be schema-valid
    and still be nonsense for a consumer.
  * docs/spec/1.0/schema.mdx (ToolCallArgsEvent.delta): the concatenated
    argument text is "conventionally a JSON document, though the protocol does
    not validate it".
  * docs/spec/1.0/schema.json (StateDeltaEvent.delta): "Structural validity
    here does not mean the patch applies ... which RFC 6902 leaves to the
    applier."

This tool reads a *recorded* stream (JSONL or SSE) offline, checks those
cross-event invariants, and optionally replays STATE_SNAPSHOT / STATE_DELTA to
a final state. No network. No SDK. Python 3.8+ standard library only.

Exit codes: 0 = clean, 1 = findings, 2 = usage or input error.
"""

import argparse
import json
import re
import sys

VERSION = "1.0.0"

# EventType enum, docs/spec/1.0/schema.json -> $defs.EventType
EVENT_TYPES = (
    "TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END",
    "TEXT_MESSAGE_CHUNK", "TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END",
    "TOOL_CALL_CHUNK", "TOOL_CALL_RESULT", "STATE_SNAPSHOT", "STATE_DELTA",
    "MESSAGES_SNAPSHOT", "ACTIVITY_SNAPSHOT", "ACTIVITY_DELTA", "RAW",
    "CUSTOM", "RUN_STARTED", "RUN_FINISHED", "RUN_ERROR", "STEP_STARTED",
    "STEP_FINISHED", "REASONING_START", "REASONING_MESSAGE_START",
    "REASONING_MESSAGE_CONTENT", "REASONING_MESSAGE_END",
    "REASONING_MESSAGE_CHUNK", "REASONING_END", "REASONING_ENCRYPTED_VALUE",
    "SUBAGENT_STARTED", "SUBAGENT_FINISHED", "SUBAGENT_ERROR",
)

# required fields per event definition (excluding the always-required "type")
REQUIRED = {
    "RUN_STARTED": ("threadId", "runId"),
    "RUN_FINISHED": ("threadId", "runId"),
    "RUN_ERROR": ("message",),
    "STEP_STARTED": ("stepName",),
    "STEP_FINISHED": ("stepName",),
    "TEXT_MESSAGE_START": ("messageId",),
    "TEXT_MESSAGE_CONTENT": ("messageId", "delta"),
    "TEXT_MESSAGE_END": ("messageId",),
    "TOOL_CALL_START": ("toolCallId", "toolCallName"),
    "TOOL_CALL_ARGS": ("toolCallId", "delta"),
    "TOOL_CALL_END": ("toolCallId",),
    "TOOL_CALL_RESULT": ("messageId", "toolCallId", "content"),
    "STATE_SNAPSHOT": ("snapshot",),
    "STATE_DELTA": ("delta",),
}

SERVER_EVENT_TYPES = ("RUN_STARTED", "RUN_FINISHED", "RUN_ERROR")
TERMINAL = ("RUN_FINISHED", "RUN_ERROR")
PATCH_OPS = ("add", "remove", "replace", "move", "copy", "test")


class Finding:
    __slots__ = ("code", "severity", "index", "etype", "detail")

    def __init__(self, code, severity, index, etype, detail):
        self.code = code
        self.severity = severity
        self.index = index
        self.etype = etype
        self.detail = detail

    def as_dict(self):
        return {
            "code": self.code,
            "severity": self.severity,
            "event_index": self.index,
            "event_type": self.etype,
            "detail": self.detail,
        }


# ---------------------------------------------------------------- input parsing

_SSE_DATA = re.compile(r"^data:\s?(.*)$")


def parse_stream(text, fmt="auto"):
    """Return (events, parse_errors). Raises ValueError on empty input."""
    stripped = text.strip()
    if not stripped:
        raise ValueError("input is empty")
    looks_sse = fmt == "sse" or (fmt == "auto" and stripped.startswith("data:"))
    events, errors = [], []
    if looks_sse:
        for lineno, raw in enumerate(text.splitlines(), 1):
            m = _SSE_DATA.match(raw.rstrip("\r"))
            if not m:
                continue
            payload = m.group(1).strip()
            if not payload or payload == "[DONE]":
                continue
            try:
                events.append(json.loads(payload))
            except json.JSONDecodeError as exc:
                errors.append({"line": lineno, "detail": "SSE data is not JSON: %s" % exc})
    else:
        for lineno, raw in enumerate(text.splitlines(), 1):
            if not raw.strip():
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                errors.append({"line": lineno, "detail": "line is not JSON: %s" % exc})
                continue
            if isinstance(obj, list):
                events.extend(obj)
            else:
                events.append(obj)
    return events, errors


# ------------------------------------------------------------- json patch (6902)


def json_pointer_tokens(pointer):
    if pointer == "":
        return []
    if not pointer.startswith("/"):
        raise ValueError("pointer must start with '/': %r" % pointer)
    return [t.replace("~1", "/").replace("~0", "~") for t in pointer[1:].split("/")]


def pointer_get(doc, pointer):
    cur = doc
    for tok in json_pointer_tokens(pointer):
        if isinstance(cur, list):
            if not tok.isdigit() or int(tok) >= len(cur):
                raise KeyError("index %r out of range" % tok)
            cur = cur[int(tok)]
        elif isinstance(cur, dict):
            if tok not in cur:
                raise KeyError("key %r missing" % tok)
            cur = cur[tok]
        else:
            raise KeyError("cannot descend into %s" % type(cur).__name__)
    return cur


def _parent(doc, pointer):
    toks = json_pointer_tokens(pointer)
    if not toks:
        raise KeyError("root has no parent")
    parent = pointer_get(doc, "/" + "/".join(t.replace("~", "~0").replace("/", "~1") for t in toks[:-1])) if len(toks) > 1 else doc
    return parent, toks[-1]


def pointer_set(doc, pointer, value, remove=False):
    toks = json_pointer_tokens(pointer)
    if not toks:
        raise KeyError("cannot address the document root")
    parent = doc
    for tok in toks[:-1]:
        if isinstance(parent, list):
            if not tok.isdigit() or int(tok) >= len(parent):
                raise KeyError("index %r out of range" % tok)
            parent = parent[int(tok)]
        elif isinstance(parent, dict):
            if tok not in parent:
                raise KeyError("key %r missing" % tok)
            parent = parent[tok]
        else:
            raise KeyError("cannot descend into %s" % type(parent).__name__)
    leaf = toks[-1]
    if isinstance(parent, list):
        if remove:
            if not leaf.isdigit() or int(leaf) >= len(parent):
                raise KeyError("index %r out of range" % leaf)
            parent.pop(int(leaf))
            return
        if leaf == "-":
            parent.append(value)
            return
        if not leaf.isdigit() or int(leaf) > len(parent):
            raise KeyError("index %r out of range" % leaf)
        idx = int(leaf)
        if idx == len(parent):
            parent.append(value)
        else:
            parent[idx] = value
        return
    if isinstance(parent, dict):
        if remove:
            if leaf not in parent:
                raise KeyError("key %r missing" % leaf)
            del parent[leaf]
            return
        parent[leaf] = value
        return
    raise KeyError("cannot patch %s" % type(parent).__name__)


def apply_patch(state, ops):
    """Apply an RFC 6902 patch. Returns (state, error_or_None)."""
    if not isinstance(ops, list):
        return state, "delta is not an array of patch operations"
    for i, op in enumerate(ops):
        if not isinstance(op, dict):
            return state, "op[%d] is not an object" % i
        kind = op.get("op")
        if kind not in PATCH_OPS:
            return state, "op[%d] has unknown op %r" % (i, kind)
        if "path" not in op or not isinstance(op["path"], str):
            return state, "op[%d] has no string path" % i
        if kind in ("add", "replace", "test") and "value" not in op:
            return state, "op[%d] (%s) has no value" % (i, kind)
        try:
            if kind == "test":
                if pointer_get(state, op["path"]) != op["value"]:
                    return state, "op[%d] test failed at %s" % (i, op["path"])
            elif kind == "remove":
                pointer_set(state, op["path"], None, remove=True)
            elif kind == "replace":
                pointer_get(state, op["path"])  # must exist
                pointer_set(state, op["path"], op["value"])
            elif kind == "add":
                pointer_set(state, op["path"], op["value"])
            else:  # move / copy
                src = op.get("from")
                if not isinstance(src, str):
                    return state, "op[%d] (%s) has no 'from'" % (i, kind)
                value = pointer_get(state, src)
                if kind == "move":
                    pointer_set(state, src, None, remove=True)
                pointer_set(state, op["path"], value)
        except (KeyError, ValueError) as exc:
            return state, "op[%d] (%s) cannot apply: %s" % (i, kind, exc)
    return state, None


# --------------------------------------------------------------------- checking


def _outcome_problem(ev):
    outcome = ev.get("outcome")
    if outcome is None:
        return None
    if not isinstance(outcome, dict):
        return "outcome is %s, not an object" % type(outcome).__name__
    if not isinstance(outcome.get("type"), str):
        return "outcome.type is missing or not a string (the AG-UI client strips unknown ones)"
    return None


def check_events(events, replay=False):
    """Return (findings, final_state_or_None, open_run_seen)."""
    findings = []
    state = None
    seen_any_run = False
    in_run = False
    run_id = None
    open_text = {}      # messageId -> index of TEXT_MESSAGE_START
    open_tool = {}      # toolCallId -> {"index": i, "chunks": [str], "ended": False}
    open_step = {}      # stepName -> index

    def add(code, sev, i, ev, detail):
        findings.append(Finding(code, sev, i, ev.get("type") if isinstance(ev, dict) else None, detail))

    for i, ev in enumerate(events):
        if not isinstance(ev, dict):
            add("E-EVENT-SHAPE", "error", i, {"type": None}, "event is %s, not an object" % type(ev).__name__)
            continue
        etype = ev.get("type")
        if etype not in EVENT_TYPES:
            add("E-UNKNOWN-TYPE", "error", i, ev, "type %r is not one of the 31 AG-UI event types" % etype)
        for field in REQUIRED.get(etype, ()):
            if field not in ev:
                add("E-MISSING-REQUIRED", "error", i, ev, "%s requires %r" % (etype, field))

        # --- run lifecycle: only the run lifecycle is mandatory (spec)
        if etype == "RUN_STARTED":
            if in_run:
                add("E-RUN-NESTED", "error", i, ev, "RUN_STARTED while run %r is still open" % run_id)
            in_run = True
            seen_any_run = True
            run_id = ev.get("runId")
            if ev.get("threadId") is not None and ev.get("runId") is None:
                add("E-RUN-KEY", "warning", i, ev, "RUN_STARTED has no runId; later events cannot be attributed")
        elif etype in TERMINAL:
            if not in_run:
                add("E-RUN-ORDER", "error", i, ev, "%s with no open run (no RUN_STARTED before it)" % etype)
            else:
                in_run = False
                run_id = None
        elif etype in SERVER_EVENT_TYPES:
            pass
        elif not in_run:
            add("E-RUN-ORDER", "error", i, ev, "%s outside a run (only the run lifecycle is mandatory, so this stream is not replayable)" % etype)

        if etype == "RUN_FINISHED":
            problem = _outcome_problem(ev)
            if problem:
                add("E-RUN-OUTCOME", "error", i, ev, problem)

        # --- text messages
        if etype == "TEXT_MESSAGE_START":
            mid = ev.get("messageId")
            if mid in open_text:
                add("E-TEXT-DUP-START", "error", i, ev, "message %r already open" % mid)
            open_text[mid] = i
        elif etype == "TEXT_MESSAGE_CONTENT":
            mid = ev.get("messageId")
            if mid not in open_text:
                add("E-TEXT-UNOPENED", "error", i, ev, "content for message %r that was never started" % mid)
        elif etype == "TEXT_MESSAGE_END":
            mid = ev.get("messageId")
            if mid not in open_text:
                add("E-TEXT-UNOPENED", "error", i, ev, "end for message %r that was never started" % mid)
            else:
                del open_text[mid]
        elif etype == "TEXT_MESSAGE_CHUNK":
            if ev.get("messageId") is not None and ev.get("delta") is not None:
                open_text.setdefault(ev["messageId"], i)

        # --- tool calls
        if etype == "TOOL_CALL_START":
            tid = ev.get("toolCallId")
            if tid in open_tool:
                add("E-TOOL-DUP-START", "error", i, ev, "tool call %r already open" % tid)
            open_tool[tid] = {"index": i, "chunks": [], "ended": False}
        elif etype == "TOOL_CALL_ARGS":
            tid = ev.get("toolCallId")
            rec = open_tool.get(tid)
            if rec is None:
                add("E-TOOL-UNOPENED", "error", i, ev, "arguments for tool call %r that was never started" % tid)
            elif rec["ended"]:
                add("E-TOOL-ARGS-AFTER-END", "error", i, ev, "arguments after TOOL_CALL_END for %r" % tid)
            else:
                rec["chunks"].append(ev.get("delta") if isinstance(ev.get("delta"), str) else "")
        elif etype == "TOOL_CALL_END":
            tid = ev.get("toolCallId")
            rec = open_tool.get(tid)
            if rec is None:
                add("E-TOOL-UNOPENED", "error", i, ev, "end for tool call %r that was never started" % tid)
            else:
                rec["ended"] = True
                text = "".join(rec["chunks"])
                if text.strip() == "":
                    add("E-TOOL-ARGS-EMPTY", "warning", i, ev, "tool call %r closed with no argument text" % tid)
                else:
                    try:
                        json.loads(text)
                    except json.JSONDecodeError as exc:
                        add("E-TOOL-ARGS-NOT-JSON", "error", i, ev,
                            "concatenated arguments for %r are not JSON (%s); the protocol does not validate this" % (tid, exc.msg))
                del open_tool[tid]
        elif etype == "TOOL_CALL_CHUNK":
            tid = ev.get("toolCallId")
            if tid is not None and tid not in open_tool:
                open_tool[tid] = {"index": i, "chunks": [ev.get("delta") or ""], "ended": False}
            elif tid is not None:
                open_tool[tid]["chunks"].append(ev.get("delta") or "")

        # --- steps
        if etype == "STEP_STARTED":
            open_step[ev.get("stepName")] = i
        elif etype == "STEP_FINISHED":
            open_step.pop(ev.get("stepName"), None)

        # --- state
        if etype == "STATE_SNAPSHOT":
            if "snapshot" in ev:
                state = ev["snapshot"]
        elif etype == "STATE_DELTA":
            if "delta" in ev:
                if state is None:
                    add("E-STATE-DELTA-NO-BASE", "error", i, ev,
                        "STATE_DELTA before any STATE_SNAPSHOT; RFC 6902 leaves applicability to the applier")
                    state = {}
                state, problem = apply_patch(state, ev["delta"])
                if problem:
                    add("E-STATE-PATCH-FAILED", "error", i, ev, problem)

    for mid, idx in open_text.items():
        findings.append(Finding("E-TEXT-UNCLOSED", "warning", idx, "TEXT_MESSAGE_START", "message %r never closed" % mid))
    for tid, rec in open_tool.items():
        findings.append(Finding("E-TOOL-UNCLOSED", "warning", rec["index"], "TOOL_CALL_START", "tool call %r never closed" % tid))
    for name, idx in open_step.items():
        findings.append(Finding("E-STEP-UNCLOSED", "warning", idx, "STEP_STARTED", "step %r never finished" % name))
    if in_run:
        findings.append(Finding("E-RUN-UNCLOSED", "error", len(events) - 1 if events else 0, "RUN_STARTED",
                                "stream ends while run %r is still open" % run_id))
    if events and not seen_any_run:
        findings.append(Finding("E-NO-RUN", "error", 0, None, "no RUN_STARTED anywhere: the stream has no lifecycle"))
    findings.sort(key=lambda f: (f.index, f.code))
    return findings, (state if replay else None)


# ------------------------------------------------------------------------ output


def build_report(path, events, parse_errors, findings, state, fmt):
    errors = sum(1 for f in findings if f.severity == "error")
    warnings = sum(1 for f in findings if f.severity == "warning")
    return {
        "tool": "agui-stream-doctor",
        "version": VERSION,
        "spec": "AG-UI 1.0",
        "input": path,
        "format": fmt,
        "events": len(events),
        "ok": (errors == 0 and not parse_errors),
        "counts": {"error": errors, "warning": warnings},
        "findings": [f.as_dict() for f in findings],
        "parse_errors": parse_errors,
        "final_state": state,
    }


def print_human(report, stream):
    if report["parse_errors"]:
        for pe in report["parse_errors"]:
            print("line %s: %s" % (pe["line"], pe["detail"]), file=stream)
    for f in report["findings"]:
        print("%s [%s] #%s %s: %s" % (f["severity"].upper(), f["code"], f["event_index"],
                                      f["event_type"] or "-", f["detail"]), file=stream)
    if report["final_state"] is not None:
        print("final state: %s" % json.dumps(report["final_state"], sort_keys=True), file=stream)
    print("%d event(s), %d error(s), %d warning(s) -> %s" % (
        report["events"], report["counts"]["error"], report["counts"]["warning"],
        "OK" if report["ok"] else "PROBLEMS"), file=stream)


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="agui-stream-doctor",
        description="Offline stream-level checker and state replayer for recorded AG-UI 1.0 event streams.")
    p.add_argument("stream", help="recorded stream: .jsonl (one event per line) or .sse (data: lines)")
    p.add_argument("--format", choices=("auto", "jsonl", "sse"), default="auto")
    p.add_argument("--json", action="store_true", help="emit a machine-readable report on stdout")
    p.add_argument("--replay", action="store_true", help="apply STATE_SNAPSHOT/STATE_DELTA and report final state")
    p.add_argument("--fail-on", choices=("error", "warning", "never"), default="error",
                   help="finding severity that makes the exit code 1 (default: error)")
    p.add_argument("--version", action="version", version="%(prog)s " + VERSION)
    args = p.parse_args(argv)

    try:
        with open(args.stream, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        print("cannot read %s: %s" % (args.stream, exc), file=sys.stderr)
        return 2
    try:
        events, parse_errors = parse_stream(text, args.format)
    except ValueError as exc:
        print("input error: %s" % exc, file=sys.stderr)
        return 2
    if parse_errors and not events:
        print("input error: no event could be parsed (%d bad line(s))" % len(parse_errors), file=sys.stderr)
        return 2

    findings, state = check_events(events, replay=args.replay)
    report = build_report(args.stream, events, parse_errors, findings, state, args.format)

    if args.json:
        json.dump(report, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    else:
        print_human(report, sys.stdout)

    if args.fail_on == "never":
        return 0
    if args.fail_on == "warning":
        return 1 if (report["counts"]["warning"] or report["counts"]["error"] or parse_errors) else 0
    return 1 if (report["counts"]["error"] or parse_errors) else 0


if __name__ == "__main__":
    sys.exit(main())
