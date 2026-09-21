# Klient GSI

Lokalny serwer odbiera telemetrie z Counter-Strike 2, odsiewa wszystko poza
aktywna gra naszego gracza i wysyla znormalizowane zdarzenia do Azure Event
Hubs.

## Instalacja

```powershell
python -m venv venv
venv\Scripts\pip install -r requirements.txt
```

Skopiuj [gsi_config_template.txt](gsi_config_template.txt) do katalogu gry jako
`...\Counter-Strike Global Offensive\game\csgo\cfg\gamestate_integration_cs2tracker.cfg`
i uruchom serwer:

```powershell
venv\Scripts\python gsi_server.py
```

Stan serwera podglada sie pod `http://127.0.0.1:3000/health` (liczniki zdarzen,
biezacy `match_id`, dlugosc kolejki wysylkowej).

## Co jest wysylane

Dwa rodzaje zdarzen, obydwa w tej samej kopercie (`schema_version`,
`event_type`, `event_id`, `match_id`, `seq`, `client_ts`, `steamid`,
`round_number`, `payload`):

- **`tick`** - stan gracza probkowany kilka razy na sekunde, wylacznie gdy
  `map.phase` i `round.phase` sa `live`, a `player.steamid` zgadza sie z
  `provider.steamid`. Menu, warmup, freezetime i przerwy nie przechodza.
- **`match_start` / `round_start` / `round_end` / `match_end`** - rzadkie
  zdarzenia cyklu zycia. `round_end` niesie `win_team`, powod wygranej oraz
  ostatni znany stan naszego gracza w tej rundzie.

## Trzy pulapki GSI, ktore obsluguje ten kod

1. **Po smierci blok `player` opisuje kogos innego.** Kamera przelacza sie na
   obserwowanego gracza - raz na kamere swobodna (`spectarget` z id encji, bez
   `steamid`), raz na kompletny blok przeciwnika, nieodrozniamy strukturalnie
   od naszego. Filtr `player.steamid == provider.steamid` odsiewa oba warianty.
2. **`map.round` to licznik rund zakonczonych.** W fazie `over` jest juz
   podniesiony o jeden, wiec numer rundy trzeba cofnac.
3. **`provider.timestamp` ma rozdzielczosc jednej sekundy** i opisuje po kilka
   pakietow naraz. Kolejnosc zdarzen wyznacza `seq` razem z `client_ts`.

Zwyciezce rundy zna wylacznie faza `over`, ktora nie trafia do ticka, dlatego
zdarzenia cyklu zycia sa wykrywane na *wszystkich* pakietach, a nie tylko na
tych dotyczacych nas - bywaja rundy, w ktorych zaden pakiet `over` nie opisuje
juz naszego gracza.

Serwer zatrzymuj przez Ctrl+C - wtedy domyka trwajacy mecz zdarzeniem
`match_end`.

## Moduly

| Plik | Rola |
| --- | --- |
| [gsi_server.py](gsi_server.py) | Serwer FastAPI, na ktory gra wysyla pakiety; handler wraca natychmiast |
| [gsi/filters.py](gsi/filters.py) | Rozstrzyga, czy pakiet dotyczy naszego gracza i czy trwa aktywna runda |
| [gsi/match_state.py](gsi/match_state.py) | Zamienia strumien stanow na zdarzenia: `match_id`, numeracja rund, cykl zycia |
| [gsi/envelope.py](gsi/envelope.py) | Koperta zdarzenia i normalizacja payloadu (m.in. bronie jako tablica) |
| [gsi/sender.py](gsi/sender.py) | Kolejka `asyncio`, batche po `match_id` i retry przy wysylce do Event Hubs |
| [config.py](config.py) | Wszystkie ustawienia, kazde nadpisywalne zmienna srodowiskowa |
