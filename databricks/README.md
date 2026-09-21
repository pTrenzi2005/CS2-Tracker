# Potok Medallion (Lakeflow Declarative Pipelines)

Potok czyta zdarzenia GSI z Azure Event Hubs i przeprowadza je przez Bronze,
Silver i Gold az do schematu gwiazdy gotowego pod dashboardy.

## Wymagania wstepne

1. **Event Hubs w warstwie Standard lub wyzszej.** Warstwa Basic nie udostepnia
   endpointu Kafka, z ktorego czyta warstwa Bronze.
2. **Secret scope** (domyslnie `cs2tracker-secrets`) z kluczem
   `evh-connection-string`.
3. **Katalog i schematy Unity Catalog** zalozone notebookiem
   [01_architecture_setup.ipynb](01_architecture_setup.ipynb).
4. Potok publikuje tabele do trzech schematow naraz (`bronze`, `silver`,
   `gold`), korzystajac z w pelni kwalifikowanych nazw w dekoratorach. Wymaga
   to domyslnego trybu publikacji, czyli pola `schema` zamiast przestarzalego
   `target` - bundle ustawia je poprawnie.

## Wdrozenie

Potok jest opisany jako Databricks Asset Bundle: [databricks.yml](../databricks.yml)
w korzeniu repozytorium plus [resources/cs2_medallion.pipeline.yml](resources/cs2_medallion.pipeline.yml).

```bash
# jednorazowo
pip install databricks-cli        # albo: winget install Databricks.DatabricksCLI
databricks auth login --host https://<twoj-workspace>.azuredatabricks.net

# przed pierwszym wdrozeniem: podmien ADB_WORKSPACE_URL w databricks.yml

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run      cs2_medallion -t dev
```

Target `prod` (`-t prod`) publikuje do katalogu `cs2_prod` i wdraza sie do
`/Shared/.bundle/`, bo tryb production nie moze siedziec w katalogu domowym.

Parametry srodowiska (katalog, namespace Event Hubs, secret scope) sa
zmiennymi bundla i trafiaja do sekcji `configuration` potoku, skad kod czyta je
przez `spark.conf.get`. Ten sam kod obsluguje DEV i PROD bez zmian.

## Tabele

| Warstwa | Tabela | Typ | Ziarno |
| --- | --- | --- | --- |
| bronze | `gsi_raw_events` | streaming table | wiadomosc z Event Hubs |
| silver | `gsi_events_parsed` | streaming table | zdarzenie (koperta rozpakowana, odduplikowana) |
| silver | `player_ticks` | streaming table | tick z aktywnej rundy |
| silver | `round_events`, `match_events` | streaming table | zdarzenie cyklu zycia |
| silver | `kill_events`, `death_events`, `damage_events`, `round_player_state` | materialized view | zdarzenie wyliczone z przyrostow |
| gold | `dim_player`, `dim_map`, `dim_weapon`, `dim_round`, `dim_match` | materialized view | wymiar |
| gold | `fact_player_positions`, `fact_kills`, `fact_deaths`, `fact_damage`, `fact_round_stats`, `agg_match_summary` | materialized view | fakt |

## Stan weryfikacji

Caly potok zostal przepuszczony lokalnie przez Sparka na nagraniu meczu
wingman 2v2 na de_nuke (1336 zdarzen). Wszystkie 20 tabel buduje sie bez
bledu, a warstwa Gold zgadza sie z licznikami w grze co do sztuki: 15
zabojstw, 9 zgonow, 16 rund, 16/16 powodow wygranej, ADR 94,3. Nie jest to
jednak przebieg na Databricks - operacje czysto strumieniowe
(`dropDuplicatesWithinWatermark`, czytanie Kafki) byly w tym tescie zastapione
odpowiednikami wsadowymi.

Trzy rzeczy, ktore ta weryfikacja wykryla i ktore sa juz poprawione:

- **`round_type` bral ekwipunek z pakietu `round_start`.** GSI wysyla go
  jeszcze w freezetime, przed zakupami, wiec wartosc to zwykle sam noz (200).
  Szesc rund na szesnascie wychodzilo `unknown`, a runda z pelnym zakupem za
  3850 byla klasyfikowana jako `eco`. Teraz decyduje `equip_value_max`
  z tickow, czyli ekwipunek, z ktorym gracz faktycznie gral.
- **`is_pistol_round` zakladalo na sztywno rundy 0 i 12.** W wingmanie polowa
  konczy sie po rundzie 7, wiec pistolowka drugiej polowy (runda 8) byla
  pomijana, a oznaczana byla zwykla runda 12. Teraz pierwsza runda polowy
  wynika ze zmiany druzyny gracza, co dziala i dla MR12, i dla wingmana.
- **`final_score_ct` / `final_score_t` bralo tablice wynikow z GSI.** Ta
  zmienia znaczenie po zmianie stron, a snapshot z ostatniej rundy nie obejmuje
  jej samej - wychodzilo 8:7 przy szesnastu rozegranych rundach. Kolumny
  nazywaja sie teraz `rounds_won_ct_side` i `rounds_won_t_side`, licza sie ze
  zwyciezcow rund i zawsze sumuja do `rounds_played`.

## Dlaczego zdarzenia pochodne to materialized view

GSI nie wysyla zdarzen, tylko stan. Zabojstwo poznajemy po wzroscie licznika
`round_kills`, smierc po zejsciu zycia do zera, obrazenia po zmianie
`round_totaldmg`. Wszystkie te reguly opieraja sie na `lag()`, ktory wymaga
uporzadkowania calej partycji - nie da sie tego policzyc przyrostowo na
strumieniu dopisywanym. Pelne przeliczenie przy skali jednego meczu
(kilkanascie tysiecy tickow) jest tanie, a logika pozostaje odtwarzalna z
historii Bronze.

## Dwa zrodla liczb i po co oba

Ticki urywaja sie w chwili smierci gracza, bo gra przelacza wtedy kamere na
kolege z druzyny. Na nagraniu meczu na de_nuke ostatni tick az w 7 z 15 rund
pokazywal gracza przy pelnym zyciu, mimo ze runde skonczyl martwy. Gdyby
polegac wylacznie na tickach, statystyki bylyby drastycznie zanizone:
1 wykryta smierc zamiast 8, 12 zabojstw zamiast 15, 1255 obrazen zamiast 1402.

Dlatego liczby i pozycje pochodza z dwoch roznych miejsc:

- **`fact_kills`, `fact_deaths`, `fact_damage`** - zdarzenia z tickow, kazde ze
  wspolrzednymi. Pod heatmapy. Moga pomijac to, co wydarzylo sie w ostatnim
  ulamku sekundy rundy, bo dla tych zdarzen zwyczajnie nie ma probki z pozycja.
- **`fact_round_stats`** - sumy ze zdarzen `round_end`, czyli z ostatniego
  pakietu rundy. Zrodlo prawdy dla K/D, ADR i procentu headshotow; zgadza sie
  co do sztuki z licznikiem w grze. Kolumna `kills_with_position` mowi, ile z
  tych zabojstw da sie postawic na mapie.

## Co daje warstwa Gold

- **Heatmapy zabojstw i smierci** - `fact_kills` i `fact_deaths` maja `pos_x`,
  `pos_y`, `pos_z` oraz `map_key` do transformacji na radar mapy.
- **Odtworzenie sciezki w rundzie** - `fact_player_positions` posortowany po
  `time_in_round_s` w obrebie `round_key`.
- **Skutecznosc broni** - `fact_kills` zlaczone z `dim_weapon`.
- **Os czasu meczu** - `fact_round_stats` zlaczone z `dim_round` (typ rundy,
  zwyciezca, powod wygranej, czas przezycia).
- **Kafelki KPI** - `agg_match_summary` z K/D, ADR, procentem headshotow i
  procentem przezytych rund.

## Maskowanie danych

Surowy SteamID64 konczy sie na warstwie Silver. W Gold wystepuje wylacznie
`player_key` (SHA-256) oraz `steamid_masked`, zgodnie z sekcja o governance w
glownym [README](../README.md).
