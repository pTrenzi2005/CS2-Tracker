"""Zapis surowych pakietow GSI na dysk, obok glownego przeplywu.

Do Event Hubs idzie tylko to, co przeszlo przez filtry. Zrzut surowy jest
niezalezny i sluzy do pracy nad samymi filtrami: mozna go w kolku odtwarzac
przez ``tests.replay`` i sprawdzac, czy odrzucamy dokladnie to, co trzeba.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class RawCapture:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.count = 0
        self._handle = None

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8")
        log.info("Zrzut surowych pakietow do %s", self.path)

    def write(self, payload: dict[str, Any]) -> None:
        if self._handle is None:
            return
        self._handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        # Nagranie meczu zwykle konczy sie zamknieciem okna, nie czystym
        # shutdownem, wiec nie ryzykujemy bufora.
        self._handle.flush()
        self.count += 1

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None
            log.info("Zapisano %s surowych pakietow", self.count)
