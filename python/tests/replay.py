"""Odtwarza zapisane pakiety GSI przez ten sam kod, ktory obsluguje zywa gre.

    python -m tests.replay --output output/replay.jsonl
    python -m tests.replay --input output/mecz_raw.jsonl

``--input`` przyjmuje katalog z plikami ``dane_*.json`` albo zrzut JSONL
zapisany przez serwer (``CS2_RAW_CAPTURE``). Bez ``--output`` wypisuje samo
podsumowanie, co pozwala sprawdzic filtry i numeracje rund bez uruchamiania
gry i bez ruchu do Azure. Tego samego kodu uzywaja fixtury w conftest.py.
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

DEFAULT_INPUT = Path(__file__).resolve().parents[1] / "test_data"

_INDEX = re.compile(r"(\d+)")


def load_payloads(directory: str | Path) -> list[dict[str, Any]]:
    """Wczytuje ``dane_*.json`` w kolejnosci numerycznej, nie leksykalnej."""
    files = sorted(
        Path(directory).glob("dane_*.json"),
        key=lambda path: int(_INDEX.search(path.stem).group(1)),
    )
    return [json.loads(path.read_text(encoding="utf-8")) for path in files]


def load_capture(path: str | Path) -> list[dict[str, Any]]:
    """Wczytuje zrzut JSONL zapisany przez serwer (``CS2_RAW_CAPTURE``)."""
    with Path(path).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def load_input(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    return load_payloads(path) if path.is_dir() else load_capture(path)


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
    parser.add_argument("--input", default=DEFAULT_INPUT)
    parser.add_argument("--output")
    args = parser.parse_args()

    payloads = load_input(args.input)
    envelopes = replay(payloads)

    by_type = Counter(envelope["event_type"] for envelope in envelopes)
    print(f"Pakiety odebrane z gry:   {len(payloads)}")
    print(f"Zdarzenia wyslane dalej:  {len(envelopes)}")
    print(f"Odfiltrowane:             {len(payloads) - by_type['tick']} pakietow nie dalo ticku")
    for event_type, number in sorted(by_type.items()):
        print(f"  {event_type:<12} {number}")

    print("\nPrzebieg meczu (bez tickow):")
    for envelope in envelopes:
        if envelope["event_type"] == "tick":
            continue
        body = envelope["payload"]
        runda = envelope["round_number"]
        runda_opis = "-" if runda is None else str(runda)
        if envelope["event_type"] == "round_end":
            przezyl = "przezyl" if body.get("player_survived") else "zginal"
            opis = f"wygrywa {body.get('win_team')}, gracz {przezyl}"
        else:
            opis = body.get("map", {}).get("name", "")
        print(f"  seq={envelope['seq']:<4} {envelope['event_type']:<12} runda={runda_opis:<4} {opis}")

    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for envelope in envelopes:
                handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        print(f"\nZapisano {len(envelopes)} zdarzen do {path}")


if __name__ == "__main__":
    main()
