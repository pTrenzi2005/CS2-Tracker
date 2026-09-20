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
   `gold`), korzystajac z w pelni kwalifikowanych nazw w dekoratorach.

## Uruchomienie

```bash
databricks pipelines create --json @databricks/pipeline.json
databricks pipelines start-update --pipeline-id <id>
```

Parametry srodowiska (katalog, namespace Event Hubs, secret scope) siedza w
sekcji `configuration` w [pipeline.json](pipeline.json) i sa odczytywane w
kodzie przez `spark.conf.get`, wiec ten sam kod obsluguje DEV i PROD.

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

## Dlaczego zdarzenia pochodne to materialized view

GSI nie wysyla zdarzen, tylko stan. Zabojstwo poznajemy po wzroscie licznika
`round_kills`, smierc po zejsciu zycia do zera, obrazenia po zmianie
`round_totaldmg`. Wszystkie te reguly opieraja sie na `lag()`, ktory wymaga
uporzadkowania calej partycji - nie da sie tego policzyc przyrostowo na
strumieniu dopisywanym. Pelne przeliczenie przy skali jednego meczu
(kilkanascie tysiecy tickow) jest tanie, a logika pozostaje odtwarzalna z
historii Bronze.

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
