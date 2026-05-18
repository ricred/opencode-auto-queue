#!/usr/bin/env python3
"""opencode queue CLI — manage .git/queue.json per project.

Usage:
  q status [-s SESSION]         Show queue status (default if no action)
  q hold                        Set mode to hold
  q immediate                   Set mode to immediate
  q clear [-s SESSION]          Clear all items
  q drop [INDEX] [-s SESSION]   Drop item at INDEX (default 1)
  q peek [-s SESSION]           Show next item
  q retry [-s SESSION]          Reset all failed items to queued
  q pause [-s SESSION]          Pause auto-drain
  q resume [-s SESSION]         Resume auto-drain
  q count [-s SESSION]          Show pending count
  q config                      Show configuration
  q reorder FROM TO [-s SESSION] Move item FROM position to TO
  q insert INDEX TEXT [-s SESSION] Insert text at INDEX
  q append TEXT [-s SESSION]    Append text to end of queue
  q prepend TEXT [-s SESSION]   Prepend text to front of queue
  q delete [INDEX] [-s SESSION] Delete item at INDEX (default 1)
  q set INDEX TEXT [-s SESSION] Replace item at INDEX with new text
  q sort [-s SESSION]           Sort by enqueue time (oldest first)
  q invert [-s SESSION]         Reverse queue order
  q get [INDEX] [-s SESSION]    Show full details of item at INDEX
  q sessions                    List all session keys with item counts

Session targeting:
  -s, --session SESSION   Target a specific session queue.
                          SESSION can be:
                            - A session ID string (e.g. "abc123def")
                            - A numeric index (1-based) into the session list
                          Without -s, targets the first session with items,
                          or "default" if no sessions have items.
"""

import json
import os
import sys
import time
from pathlib import Path

QUEUE_FILE = Path(".git") / "queue.json"
VERSION = 1


def find_queue_file() -> Path:
    cwd = Path.cwd()
    while cwd != cwd.parent:
        candidate = cwd / ".git" / "queue.json"
        if candidate.exists():
            return candidate
        cwd = cwd.parent
    return Path.cwd() / ".git" / "queue.json"


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"version": VERSION, "mode": "hold", "queues": {}, "pausedSessions": []}
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if not data or data.get("version") != 1:
            return {"version": VERSION, "mode": "hold", "queues": {}, "pausedSessions": []}
        return data
    except (json.JSONDecodeError, OSError):
        return {"version": VERSION, "mode": "hold", "queues": {}, "pausedSessions": []}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


def session_keys(state: dict) -> list[str]:
    return list(state.get("queues", {}).keys())


def resolve_session(state: dict, session_ref: str | None) -> str:
    """Resolve a session reference (ID or numeric index) to a session key.

    If session_ref is None, returns the first session with items, or "default".
    """
    queues = state.get("queues", {})
    keys = list(queues.keys())

    if session_ref is None:
        for k in keys:
            if queues[k]:
                return k
        return keys[0] if keys else "default"

    try:
        idx = int(session_ref)
        if idx < 1 or idx > len(keys):
            raise IndexError(f"Session index {idx} out of range (1-{len(keys)})")
        return keys[idx - 1]
    except ValueError:
        pass

    if session_ref in queues:
        return session_ref

    for k in keys:
        if k.startswith(session_ref) or session_ref in k:
            return k

    return session_ref


def get_queue(state: dict, session_key: str) -> list:
    return state.get("queues", {}).get(session_key, [])


def set_queue(state: dict, session_key: str, queue: list) -> dict:
    queues = state.setdefault("queues", {})
    queues[session_key] = queue
    return state


def ensure_queue(state: dict, session_key: str) -> dict:
    queues = state.setdefault("queues", {})
    if session_key not in queues:
        queues[session_key] = []
    return state


def pending_count(queue: list) -> int:
    return sum(1 for i in queue if i.get("status") in ("queued", "sending", "failed"))


def failed_count(queue: list) -> int:
    return sum(1 for i in queue if i.get("status") == "failed")


def make_item(text: str, session_key: str = "") -> dict:
    return {
        "sessionID": session_key,
        "parts": [{"type": "text", "text": text}],
        "preview": text[:28] + ("..." if len(text) > 28 else ""),
        "status": "queued",
        "retries": 0,
        "enqueuedAt": int(time.time() * 1000),
    }


def fmt_age(ms: int) -> str:
    s = int((time.time() * 1000 - ms) / 1000)
    if s < 60:
        return f"{s}s ago"
    m = s // 60
    if m < 60:
        return f"{m}m ago"
    h = m // 60
    return f"{h}h {m % 60}m ago"


def fmt_item_line(i: int, item: dict) -> str:
    s = item.get("status", "queued")
    icon = {"sent": "[x]", "sending": "[>]", "failed": "[!]"}.get(s, "[ ]")
    preview = item.get("preview", "?")
    retries = item.get("retries", 0)
    retry_str = f" ({retries} retries)" if retries > 0 else ""
    age = fmt_age(item.get("enqueuedAt", 0))
    return f"  {i}. {icon} {preview}{retry_str} [{age}]"


def cmd_sessions(state: dict) -> str:
    queues = state.get("queues", {})
    keys = list(queues.keys())
    if not keys:
        return "No sessions."
    lines = []
    for i, k in enumerate(keys, 1):
        q = queues[k]
        pc = pending_count(q)
        fc = failed_count(q)
        count_str = f"{pc} pending" if pc else "empty"
        fail_str = f", {fc} failed" if fc else ""
        lines.append(f"  {i}. {k}  ({count_str}{fail_str}, {len(q)} total)")
    return "Sessions:\n" + "\n".join(lines)


def cmd_status(state: dict, path: Path, sk: str) -> str:
    mode = state.get("mode", "hold")
    paused = state.get("pausedSessions", [])
    queue = get_queue(state, sk)
    pc = pending_count(queue)
    fc = failed_count(queue)
    keys = session_keys(state)
    sk_label = f"@{sk}" if len(keys) > 1 else ""
    lines = [
        f"Mode: {mode}",
        f"Paused: {bool(paused)}",
        f"Session: {sk}{sk_label}",
        f"Queued: {pc}",
        f"Failed: {fc}",
        f"File: {path}",
    ]
    if queue:
        lines.append("")
        lines.append("Queue:")
        for i, item in enumerate(queue, 1):
            lines.append(fmt_item_line(i, item))
    return "\n".join(lines)


def cmd_hold(state: dict) -> str:
    if state.get("mode") == "hold":
        return "Mode: hold (already active)"
    state["mode"] = "hold"
    return "Mode: hold (queued messages held until manually drained)"


def cmd_immediate(state: dict) -> str:
    if state.get("mode") == "immediate":
        return "Mode: immediate (already active)"
    state["mode"] = "immediate"
    return "Mode: immediate (queued messages auto-drain on idle)"


def cmd_clear(state: dict, sk: str) -> str:
    queue = get_queue(state, sk)
    cleared = len(queue)
    set_queue(state, sk, [])
    return f"Cleared {cleared} messages from {sk}"


def cmd_drop(state: dict, sk: str, index: int) -> str:
    queue = get_queue(state, sk)
    idx = index - 1
    if idx < 0 or idx >= len(queue):
        return f"Invalid index. Queue has {len(queue)} items."
    dropped = queue.pop(idx)
    set_queue(state, sk, queue)
    return f"Dropped: {dropped.get('preview', '?')}"


def cmd_peek(state: dict, sk: str) -> str:
    queue = get_queue(state, sk)
    for item in queue:
        if item.get("status") in ("queued", "failed"):
            preview = item.get("preview", "?")
            retries = item.get("retries", 0)
            retry_str = f" (retried {retries}x)" if retries > 0 else ""
            age = fmt_age(item.get("enqueuedAt", 0))
            return f"Next: {preview}{retry_str} [waiting {age}]"
    return "Queue is empty"


def cmd_retry(state: dict, sk: str) -> str:
    queue = get_queue(state, sk)
    failed = [i for i in queue if i.get("status") == "failed"]
    if not failed:
        return "No failed items to retry."
    for item in failed:
        item["status"] = "queued"
        item["retries"] = 0
        item.pop("lastError", None)
    set_queue(state, sk, queue)
    return f"Retrying {len(failed)} failed messages"


def cmd_pause(state: dict, sk: str) -> str:
    paused = state.setdefault("pausedSessions", [])
    if sk not in paused:
        paused.append(sk)
    return f"Queue paused for {sk}."


def cmd_resume(state: dict, sk: str) -> str:
    paused = state.get("pausedSessions", [])
    state["pausedSessions"] = [s for s in paused if s != sk]
    return f"Queue resumed for {sk}."


def cmd_count(state: dict, sk: str) -> str:
    queue = get_queue(state, sk)
    pc = pending_count(queue)
    fc = failed_count(queue)
    return f"{pc} messages in queue ({fc} failed)"


def cmd_config(state: dict, path: Path) -> str:
    keys = session_keys(state)
    sessions_detail = []
    for k in keys:
        q = state.get("queues", {}).get(k, [])
        sessions_detail.append(f"    {k}: {len(q)} items ({pending_count(q)} pending)")
    sessions_block = "\n".join(sessions_detail) if sessions_detail else "    (none)"
    return "\n".join([
        "Queue Configuration:",
        f"  mode: {state.get('mode', 'hold')}",
        f"  persistPath: {path}",
        f"  pausedSessions: {state.get('pausedSessions', [])}",
        f"  sessions ({len(keys)}):",
        sessions_block,
    ])


def cmd_reorder(state: dict, sk: str, frm: int, to: int) -> str:
    queue = get_queue(state, sk)
    if frm < 1 or frm > len(queue) or to < 1 or to > len(queue):
        return f"Index out of range. Queue has {len(queue)} items."
    item = queue.pop(frm - 1)
    queue.insert(to - 1, item)
    set_queue(state, sk, queue)
    return f"Moved item {frm} to position {to}"


def cmd_insert(state: dict, sk: str, index: int, text: str) -> str:
    queue = get_queue(state, sk)
    if index < 1 or index > len(queue) + 1:
        return f"Index out of range. Queue has {len(queue)} items."
    item = make_item(text, sk)
    queue.insert(index - 1, item)
    set_queue(state, sk, queue)
    return f"Inserted at position {index}: {item['preview']}"


def cmd_append(state: dict, sk: str, text: str) -> str:
    queue = get_queue(state, sk)
    item = make_item(text, sk)
    queue.append(item)
    set_queue(state, sk, queue)
    return f"Appended: {item['preview']}"


def cmd_prepend(state: dict, sk: str, text: str) -> str:
    queue = get_queue(state, sk)
    item = make_item(text, sk)
    queue.insert(0, item)
    set_queue(state, sk, queue)
    return f"Prepended: {item['preview']}"


def cmd_delete(state: dict, sk: str, index: int) -> str:
    queue = get_queue(state, sk)
    idx = index - 1
    if idx < 0 or idx >= len(queue):
        return f"Index out of range. Queue has {len(queue)} items."
    dropped = queue.pop(idx)
    set_queue(state, sk, queue)
    return f"Deleted: {dropped.get('preview', '?')}"


def cmd_set(state: dict, sk: str, index: int, text: str) -> str:
    queue = get_queue(state, sk)
    if index < 1 or index > len(queue):
        return f"Index out of range. Queue has {len(queue)} items."
    item = make_item(text, sk)
    queue[index - 1] = item
    set_queue(state, sk, queue)
    return f"Set position {index}: {item['preview']}"


def cmd_sort(state: dict, sk: str) -> str:
    queue = get_queue(state, sk)
    queue.sort(key=lambda i: i.get("enqueuedAt", 0))
    set_queue(state, sk, queue)
    return f"Sorted {len(queue)} items by time (oldest first)"


def cmd_invert(state: dict, sk: str) -> str:
    queue = get_queue(state, sk)
    queue.reverse()
    set_queue(state, sk, queue)
    return f"Reversed {len(queue)} items"


def cmd_get(state: dict, sk: str, index: int) -> str:
    queue = get_queue(state, sk)
    if index < 1 or index > len(queue):
        return f"Index out of range. Queue has {len(queue)} items."
    item = queue[index - 1]
    age = fmt_age(item.get("enqueuedAt", 0))
    retries = item.get("retries", 0)
    last_err = item.get("lastError", "")
    retry_str = f"\nRetries: {retries}" if retries > 0 else ""
    err_str = f"\nError: {last_err}" if last_err else ""
    text_parts = [p.get("text", "") for p in item.get("parts", []) if p.get("type") == "text"]
    content = "\n".join(text_parts) if text_parts else "(no text content)"
    return "\n".join([
        f"Item {index}/{len(queue)} @ {sk} status: {item.get('status', '?')} age: {age}{retry_str}{err_str}",
        f"Preview: {item.get('preview', '?')}",
        "Content:",
        content,
    ])


def parse_index(arg: str) -> int | None:
    try:
        n = int(arg)
        return n if n > 0 else None
    except (ValueError, TypeError):
        return None


def parse_args(args: list[str]) -> tuple[str, list[str], str | None]:
    """Parse action, positional args, and -s/--session flag.

    Supports -s before or after action:
      q -s 2 status
      q status -s 2
      q -s abc123 peek
    """
    session_ref = None
    positional = []
    i = 0
    while i < len(args):
        if args[i] in ("-s", "--session") and i + 1 < len(args):
            session_ref = args[i + 1]
            i += 2
        elif args[i] in ("-s", "--session"):
            print("Error: -s/--session requires a value", file=sys.stderr)
            sys.exit(1)
        else:
            positional.append(args[i])
            i += 1
    action = positional[0] if positional else "status"
    return action, positional[1:], session_ref


def main():
    args = sys.argv[1:]
    path = find_queue_file()
    state = load_state(path)
    needs_save = False

    action, pos, session_ref = parse_args(args)
    try:
        sk = resolve_session(state, session_ref)
    except (IndexError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        print("Use 'q sessions' to list available sessions.", file=sys.stderr)
        sys.exit(1)
    ensure_queue(state, sk)

    result = ""
    match action:
        case "sessions" | "ls" | "list":
            result = cmd_sessions(state)
        case "status":
            result = cmd_status(state, path, sk)
        case "hold":
            result = cmd_hold(state)
            needs_save = True
        case "immediate":
            result = cmd_immediate(state)
            needs_save = True
        case "clear":
            result = cmd_clear(state, sk)
            needs_save = True
        case "drop":
            idx = parse_index(pos[0]) if pos else 1
            if idx is None:
                result = "Invalid index."
            else:
                result = cmd_drop(state, sk, idx)
                needs_save = True
        case "peek":
            result = cmd_peek(state, sk)
        case "retry":
            result = cmd_retry(state, sk)
            needs_save = True
        case "pause":
            result = cmd_pause(state, sk)
            needs_save = True
        case "resume":
            result = cmd_resume(state, sk)
            needs_save = True
        case "count":
            result = cmd_count(state, sk)
        case "config":
            result = cmd_config(state, path)
        case "reorder":
            if len(pos) < 2:
                result = "Usage: q reorder <from> <to> [-s SESSION]"
            else:
                frm = parse_index(pos[0])
                to = parse_index(pos[1])
                if frm is None or to is None:
                    result = "Invalid indices."
                else:
                    result = cmd_reorder(state, sk, frm, to)
                    needs_save = True
        case "insert":
            if len(pos) < 2:
                result = "Usage: q insert <index> <text> [-s SESSION]"
            else:
                idx = parse_index(pos[0])
                text = " ".join(pos[1:])
                if idx is None:
                    result = "Invalid index."
                else:
                    result = cmd_insert(state, sk, idx, text)
                    needs_save = True
        case "append":
            if not pos:
                result = "Usage: q append <text> [-s SESSION]"
            else:
                text = " ".join(pos)
                result = cmd_append(state, sk, text)
                needs_save = True
        case "prepend":
            if not pos:
                result = "Usage: q prepend <text> [-s SESSION]"
            else:
                text = " ".join(pos)
                result = cmd_prepend(state, sk, text)
                needs_save = True
        case "delete":
            idx = parse_index(pos[0]) if pos else 1
            if idx is None:
                result = "Invalid index."
            else:
                result = cmd_delete(state, sk, idx)
                needs_save = True
        case "set":
            if len(pos) < 2:
                result = "Usage: q set <index> <text> [-s SESSION]"
            else:
                idx = parse_index(pos[0])
                text = " ".join(pos[1:])
                if idx is None:
                    result = "Invalid index."
                else:
                    result = cmd_set(state, sk, idx, text)
                    needs_save = True
        case "sort":
            result = cmd_sort(state, sk)
            needs_save = True
        case "invert":
            result = cmd_invert(state, sk)
            needs_save = True
        case "get":
            idx = parse_index(pos[0]) if pos else 1
            if idx is None:
                result = "Invalid index."
            else:
                result = cmd_get(state, sk, idx)
        case _:
            result = f"Unknown action: {action}\nTry: status hold immediate clear drop peek retry pause resume count config reorder insert append prepend delete set sort invert get sessions"
            needs_save = False

    if needs_save:
        save_state(path, state)

    print(result)


if __name__ == "__main__":
    main()
