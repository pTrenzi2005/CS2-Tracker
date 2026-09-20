import json
import sys
from pathlib import Path

import pytest

PYTHON_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIR))

TEST_DATA = PYTHON_DIR / "test_data"


@pytest.fixture(scope="session")
def payloads():
    from replay import load_payloads

    if not TEST_DATA.exists():
        pytest.skip("Brak katalogu test_data z nagranymi pakietami GSI")
    return load_payloads(TEST_DATA)


@pytest.fixture(scope="session")
def envelopes(payloads):
    from replay import replay

    return replay(payloads)


@pytest.fixture
def packet():
    if not TEST_DATA.exists():
        pytest.skip("Brak katalogu test_data z nagranymi pakietami GSI")

    def _load(index: int) -> dict:
        return json.loads((TEST_DATA / f"dane_{index}.json").read_text(encoding="utf-8"))

    return _load
