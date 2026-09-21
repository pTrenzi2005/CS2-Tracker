import json
from pathlib import Path

import pytest

from tests.replay import DEFAULT_INPUT, load_payloads, replay

TEST_DATA = DEFAULT_INPUT


@pytest.fixture(scope="session")
def payloads():
    if not TEST_DATA.exists():
        pytest.skip("Brak katalogu test_data z nagranymi pakietami GSI")
    return load_payloads(TEST_DATA)


@pytest.fixture(scope="session")
def envelopes(payloads):
    return replay(payloads)


@pytest.fixture
def packet():
    if not TEST_DATA.exists():
        pytest.skip("Brak katalogu test_data z nagranymi pakietami GSI")

    def _load(index: int) -> dict:
        return json.loads((TEST_DATA / f"dane_{index}.json").read_text(encoding="utf-8"))

    return _load
