Dokument Projektowy: CS2 Analytics & AI Coach Platform
## 1. Streszczenie Projektu
Projekt "CS2 Analytics & AI Coach Platform" to w pełni zautomatyzowana, chmurowa platforma danych end-to-end, zbudowana w oparciu o ekosystem Databricks i Microsoft Azure. Głównym celem systemu jest pobieranie telemetrii z gry Counter-Strike 2 w czasie rzeczywistym, przetwarzanie jej zgodnie z architekturą Medallion (Bronze/Silver/Gold) oraz udostępnianie wyników za pomocą analitycznych dashboardów i asystenta AI (Trenera). Projekt demonstruje integrację danych strumieniowych i wsadowych, rygorystyczne zarządzanie dostępem (Unity Catalog) oraz wdrożenie typu "Infrastructure as Code" poprzez Databricks Asset Bundles i procesy CI/CD.

## 2. Architektura Danych i Źródła
Platforma integruje dwa główne strumienie danych:

### Dane Strumieniowe (Streaming Ingestion):

Źródło: Counter-Strike 2 Game State Integration (GSI).

Mechanizm: Silnik gry generuje pliki JSON z telemetrią (np. współrzędne gracza, oddane strzały, otrzymane obrażenia, rzucone granaty). Lokalny skrypt Python nasłuchuje zdarzeń i bezpiecznie przesyła je za pomocą protokołu HTTPS do usługi Azure Event Hubs.

### Dane Wsadowe (Batch Ingestion):

Źródło: Relacyjna baza danych (np. Azure SQL) lub statyczne pliki (CSV/JSON).

Zawartość: Statyczne metadane dotyczące gry: parametry broni oraz topologia map (koordynaty stref plantowania bomby, respawnów), statystyki.

## 3. Przetwarzanie Danych (Architektura Medallion)
Potoki danych są budowane w sposób deklaratywny (Lakeflow / Delta Live Tables) i gwarantują idempotentność oraz powtarzalność. Tabele przechowują pełną historię meczów i są partycjonowane po kluczu match_id.

### Warstwa Bronze (Surowe Dane):

Błyskawiczny zrzut surowych wiadomości JSON bezpośrednio z Azure Event Hubs. Dane nie są w żaden sposób modyfikowane, co pozwala na pełne odtworzenie historii.

### Warstwa Silver (Oczyszczanie i Walidacja):

Spłaszczenie zagnieżdżonych struktur JSON.

Wyodrębnienie specyficznych zdarzeń do osobnych tabel (np. silver_damage_events, silver_grenade_throws, silver_player_movement).

Data Quality: Implementacja kontraktów danych (expectations), np. odrzucanie rekordów z wartościami obrażeń poniżej zera lub z pustym identyfikatorem gracza.

### Warstwa Gold (Analityka i KPI):

Tabele zoptymalizowane pod odczyt dla zapytań BI. Dimension tables, fact tables i aggregation tables.

Agregacje na poziomie rundy i całego meczu: K/D Ratio, ADR (Average Damage per Round), % Headshotów itp.

Łączenie z danymi wsadowymi (np. przypisanie nazwy i klasy broni na podstawie jej ID/nazwy z warstwy streamingowej).

## 4. Wyzwalanie Zadań (REST API Automation)
Zamiast przetwarzania wsadowego opartego wyłącznie na sztywnym harmonogramie (cron), system reaguje na zdarzenia:

Po zakończeniu meczu użytkownik wywołuje endpoint Databricks REST API (np. POST /api/2.1/jobs/run-now), przekazując match_id.

Żądanie to automatycznie wyzwala Lakeflow Job, który odświeża tylko najnowsze partycje danych (przechodząc przez Bronze -> Silver -> Gold) i aktualizuje raporty końcowe natychmiast po wyjściu z serwera.

## 5. Aplikacja AI (AI Coach z wykorzystaniem RAG)
Na szczycie warstwy analitycznej działa Agent AI, zbudowany w oparciu o architekturę Retrieval-Augmented Generation (RAG):

Baza Wiedzy (Unstructured Data): Dokumentacja map, poradniki taktyczne, oficjalne "Patch Notes" od Valve wgrane jako pliki PDF/Markdown do Unity Catalog Volumes.

Wektoryzacja: Teksty są dzielone na mniejsze fragmenty (chunking), wektoryzowane i przechowywane w usłudze Vector Search.

Działanie: Gracz zadaje pytanie: "Dlaczego tak często ginę w pierwszej minucie na mapie Inferno?". Model odpytuje bazę SQL (Warstwa Gold), aby potwierdzić fakty (Agent Function Calling), a następnie zestawia je ze strategiami profesjonalistów z bazy wektorowej, generując spersonalizowaną, podpartą danymi poradę.

## 6. Zarządzanie, Bezpieczeństwo i Governance
Unity Catalog: Centralne zarządzanie całą platformą.

Row-Level & Column-Level Security (RLS/CLS): Wprowadzenie polityk maskowania. Wrażliwe identyfikatory użytkowników (SteamID64) oraz adresy IP klientów są maskowane w warstwach Silver i Gold.

Azure Key Vault: Żadne hasła, tokeny dostępowe do Event Hubs ani klucze API nie są przechowywane w kodzie źródłowym. Wszystkie sekrety (Secret Scopes) są pobierane w locie podczas uruchamiania klastrów.

## 7. Środowiska, CI/CD i Databricks Asset Bundles
Projekt wymusza pełną automatyzację ("nothing built by hand in PROD"):

Środowiska: Wyraźny podział na środowisko deweloperskie (DEV) oraz płatne środowisko produkcyjne (PROD).

Kod jako Infrastruktura (IaC): Wszystkie potoki, zadania, struktury tabel i ustawienia uprawnień są zdefiniowane w plikach konfiguracyjnych YAML (databricks.yml) w ramach Databricks Asset Bundles.

Potok CI/CD: Kod przechowywany jest w repozytorium Git (GitHub/Azure DevOps). Wysłanie zmian (Push/Pull Request) do głównej gałęzi wyzwala systematyczne testy (unit testy logiki biznesowej, sprawdzenie jakości kodu). Po zaliczeniu testów pipeline automatycznie wykonuje databricks bundle deploy -t prod.

## 8. Kluczowe Dostarczane Elementy (Deliverables)
Repozytorium Git: Zawierające pełny kod PySpark/SQL, skrypt ingestii (GSI -> Event Hubs), pliki konfiguracyjne DABs oraz dokumentację (README.md).

Działająca Infrastruktura: Przechodzący na zielono potok CI/CD wdrażający platformę na Azure bez interwencji manualnej.

Dashboard Analityczny: Zbudowany w Databricks SQL, wyświetlający m.in. heatmapy i statystyki skuteczności z ostatnich meczów.

Aplikacja AI: Funkcjonujący Agent RAG gotowy do odpowiadania na spersonalizowane pytania treningowe.
