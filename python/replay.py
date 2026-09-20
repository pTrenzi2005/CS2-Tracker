"""Odtwarza zapisane pakiety GSI przez ten sam kod, ktory obsluguje zywa gre.

    python replay.py --input test_data --output output/replay.jsonl

Bez ``--output`` wypisuje samo podsumowanie, co pozwala sprawdzic filtry i
numeracje rund bez uruchamiania gry i bez ruchu do Azure.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from gsi.envelope import EnvelopeFactory
from gsi.match_state import DEFAULT_IDLE_TIMEOUT_S, MatchTracker

_INDEX = re.compile(r"(\d+)")


def load_payloads(directory: str | Path) -> list[dict[str, Any]]:
    """Wczytuje ``dane_*.json`` w kolejnosci numerycznej, nie leksykalnej."""
    files = sorted(
        Path(directory).glob("dane_*.json"),
        key=lambda path: int(_INDEX.search(path.stem).group(1)),
    )
    return [json.loads(path.read_text(encoding="utf-8")) for path in files]


def replay(
    payloads: Iterable[dict[str, Any]], idle_timeout_s: int = DEFAULT_IDLE_TIMEOUT_S
) -> list[dict[str, Any]]:
    tracker = MatchTracker(idle_timeout_s=idle_timeout_s)
    factory = EnvelopeFactory(session_id="replay")
    envelopes = []
    for payload in payloads:
        for event in tracker.process(payload):
            envelopes.append(factory.wrap(event))
    for event in tracker.finalize():
        envelopes.append(factory.wrap(event))
    return envelopes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="test_data")
    parser.add_argument("--output")
    args = parser.parse_args()

    payloads = load_payloads(args.input)
    envelopes = replay(payloads)

    by_type = Counter(envelope["event_type"] for envelope in envelopes)
    print(f"Pakiety wejsciowe: {len(payloads)}")
    print(f"Zdarzenia wyjsciowe: {len(envelopes)}")
    for event_type, number in sorted(by_type.items()):
        print(f"  {event_type:<12} {number}")
    for envelope in envelopes:
        if envelope["event_type"] in ("round_start", "round_end", "match_start", "match_end"):
            body = envelope["payload"]
            extra = body.get("win_team") or body.get("map", {}).get("name") or ""
            print(f"  seq={envelope['seq']:<4} {envelope['event_type']:<12} runda={envelope['round_number']} {extra}")

    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for envelope in envelopes:
                handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        print(f"Zapisano {len(envelopes)} zdarzen do {path}")


if __name__ == "__main__":
    main()
