# Anforderungen fuer eine Wallet-Anwendung mit NFT Data Platform

Dieses Dokument beschreibt die Zielarchitektur und die Anforderungen fuer eine Wallet-Anwendung, die parallel zur NFT Data Platform entwickelt wird.

Die NFT Data Platform selbst ist dabei nicht das spaetere Wallet-Frontend. Das in diesem Repository vorhandene Web-Frontend dient nur als Test-, Operator- und Integrationsoberflaeche fuer die API und den Worker.

Die Wallet-Anwendung soll:

- Wallet-Logins unterstuetzen
- die aktuell gehaltenen NFTs einer Wallet ueber eine externe oeffentliche Ownership-API ermitteln
- aus dieser externen Quelle nur die NFT-Identitaet uebernehmen
- die eigentlichen NFT-, Collection- und Media-Daten ueber die NFT Data Platform anreichern lassen

## Zielbild

Die Wallet-Anwendung nutzt zwei Datenquellen mit klar getrennten Verantwortlichkeiten:

1. Eine externe Ownership-API, zum Beispiel Alchemy, Moralis oder eine vergleichbare oeffentliche NFT-API.
2. Die NFT Data Platform als internes Enrichment-, Discover- und Read-Backend.

Die Wallet-Anwendung und die NFT Data Platform werden als getrennte Services entwickelt und deployed.

Verantwortung der externen Ownership-API:

- Ermitteln, welche NFTs eine Wallet aktuell haelt.
- Rueckgabe mindestens von `chainId`, `contractAddress` und `tokenId`.

Verantwortung der NFT Data Platform:

- Discovery und Materialisierung von Token-, Collection- und Media-Daten.
- Einheitliche Read-API fuer das Wallet-Frontend.
- Persistenz, Wiederverwendung und spaeteres schnelles Ausliefern bereits entdeckter NFTs.

## Architekturprinzip

Die Wallet-Anwendung darf die NFT-Holdings nicht aus der Data Platform ableiten.

Die Data Platform wird in dieser Architektur als eigenes Backend-Microservice behandelt, nicht als eingebetteter Teil des Wallet-Frontends.

Stattdessen gilt immer dieser Ablauf:

1. Wallet wird im Wallet-Frontend verbunden.
2. Das Wallet-Backend oder ein serverseitiger Route-Handler fragt die externe Ownership-API ab.
3. Die externe API liefert eine Liste von NFT-Identitaeten.
4. Das Wallet-Backend sendet fuer diese NFT-Identitaeten Discovery-Requests an die NFT Data Platform.
5. Der Worker der Data Platform materialisiert die Daten.
6. Das Wallet-Frontend liest die angereicherten NFT-Daten ueber die Read-API der Data Platform.

Empfohlener Einstiegspunkt fuer diesen Schritt:

- `POST /api/v1/owners/wallets/discover`

## Fachliche Anforderungen

### 1. Wallet-Login

Die Wallet-Anwendung muss Wallet-Connect fuer EVM-Wallets unterstuetzen.

Mindestanforderungen:

- Verbindung mit einer Wallet-Adresse
- saubere Abmeldung
- Erkennen der aktiven Wallet-Adresse
- Erkennen des aktiven Netzwerks

Die Wallet-Adresse ist der Primaerschluessel fuer die externe Ownership-Abfrage.

### 2. Externe Wallet-Ownership-Abfrage

Die Wallet-Anwendung muss eine externe Ownership-API verwenden, um die gehaltenen NFTs einer Wallet zu bestimmen.

Mindestanforderungen an die Rueckgabe aus dieser externen API:

- `chainId`
- `contractAddress`
- `tokenId`

Optional zusaetzlich nuetzlich:

- `standard`
- `balance`
- `ownerAddress`

Die Wallet-Anwendung darf sich bei NFT-Metadaten, Collection-Metadaten und Media-URLs nicht auf diese externe API verlassen. Diese Daten sollen aus der NFT Data Platform kommen.

### 3. Normiertes Identity-Format

Alle NFT-Identitaeten muessen intern in einem einheitlichen Format weitergegeben werden:

```json
{
  "chainId": 11155111,
  "contractAddress": "0xa7c41cea4f9195eebdc85054e6b0e799035bf02f",
  "tokenId": "4200042"
}
```

Anforderungen:

- `contractAddress` immer normalisiert als EVM-Adresse
- `tokenId` immer als String
- `chainId` immer als positive Integer-ID

### 4. Discovery ueber die NFT Data Platform

Fuer jede NFT-Identitaet muss die Wallet-Anwendung einen Discovery-Schritt ueber die Data Platform ausloesen.

Dafuer wird aktuell genutzt:

- `POST /api/v1/owners/wallets/discover`
- intern daraus abgeleitet: `POST /api/v1/refresh/token` fuer fehlende NFTs

Payload:

```json
{
  "ownerAddress": "0x1234567890abcdef1234567890abcdef12345678",
  "items": [
    {
      "chainId": 11155111,
      "contractAddress": "0xa7c41cea4f9195eebdc85054e6b0e799035bf02f",
      "tokenId": "4200042"
    }
  ]
}
```

Anforderungen fuer die Wallet-Anwendung:

- Requests muessen serverseitig signiert werden.
- Die Wallet-Anwendung darf HMAC-Secrets nicht im Browser halten.
- Doppelte Discovery-Requests muessen tolerierbar sein.
- Die Orchestrierung muss mit asynchronen Jobs umgehen koennen.

## 5. Read-API fuer angereicherte NFT-Daten

Nach dem Discovery-Schritt muss die Wallet-Anwendung die Daten ueber die Read-API abrufen.

Wichtige Endpunkte:

- `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId`
- `GET /api/v1/collections/:chainId/:contractAddress`
- `GET /api/v1/owners/wallets/:ownerAddress`
- `GET /api/v1/owners/wallets/:chainId/:ownerAddress`

Die Wallet-Anwendung muss damit rechnen, dass Discovery und Read zeitlich entkoppelt sind.

Das bedeutet:

- Ein Refresh-Request liefert nicht sofort die finalen NFT-Daten.
- Ein Token kann kurzzeitig noch `404` liefern.
- Wallet-Items koennen temporaer `token: null` oder `collection: null` enthalten.

### 6. Zustandsmodell fuer das Frontend

Das Wallet-Frontend muss pro NFT mindestens die folgenden Zustaende darstellen koennen:

- `not_requested`
- `queued`
- `loading`
- `ready`
- `partial`
- `failed`

Empfohlene Bedeutung:

- `not_requested`: NFT wurde aus der externen Ownership-API erhalten, aber noch nicht an die Data Platform geschickt.
- `queued`: Refresh-Job wurde erfolgreich eingestellt.
- `loading`: Data Platform liefert noch keine vollstaendigen Daten.
- `ready`: Token- und Collection-Daten sind verfuegbar.
- `partial`: Grunddaten sind verfuegbar, Media oder einzelne Felder fehlen noch.
- `failed`: Discovery oder Read ist fehlgeschlagen.

### 7. Polling- und Retry-Verhalten

Die Wallet-Anwendung muss nach dem Discovery pollend oder verzugsgesteuert erneut lesen koennen.

Mindestanforderungen:

- kurze Retry-Intervalle fuer frische NFTs
- Abbruch nach konfigurierbarer Maximalzeit
- Exponential Backoff oder abgestufte Polling-Intervalle
- kein permanentes aggressives Polling

Empfohlener Startwert:

- erste 3 Polls im Abstand von 1 bis 2 Sekunden
- danach 3 bis 5 Sekunden
- danach Abbruch oder Hintergrund-Refresh

### 8. Batch-Orchestrierung

Die Wallet-Anwendung muss mehrere NFTs einer Wallet parallel, aber kontrolliert entdecken koennen.

Mindestanforderungen:

- parallele Verarbeitung mehrerer NFTs
- Begrenzung der gleichzeitigen Discovery-Requests
- Deduplizierung identischer `(chainId, contractAddress, tokenId)`-Kombinationen
- Wiederaufnahme bei Teilfehlern

Empfehlung:

- eine interne Queue im Wallet-Backend
- limitierte Parallelitaet, zum Beispiel 5 bis 20 gleichzeitige Refresh-Requests

### 9. Trennung von Frontend und Backend

Die Wallet-Anwendung muss serverseitig zwischen Frontend und APIs vermitteln.

Das Frontend soll nicht direkt sprechen mit:

- der HMAC-geschuetzten NFT Data Platform API
- der externen Ownership-API, wenn deren Key geschuetzt bleiben soll

Stattdessen soll das Wallet-Backend oder ein serverseitiger BFF-Layer diese Rollen uebernehmen:

- Wallet-Ownership abrufen
- NFT-Identitaeten normieren
- Discovery an die Data Platform senden
- Ergebnisse zwischenspeichern oder aggregieren
- eine frontendsichere Response erzeugen

## Datenverantwortung

Die Wallet-Anwendung soll Daten bewusst aus unterschiedlichen Quellen beziehen.

Quelle: Externe Ownership-API

- Welche NFTs haelt die Wallet?
- Welche `contractAddress` und `tokenId` muessen entdeckt werden?

Quelle: NFT Data Platform

- Name des Tokens
- Beschreibung
- Attribute
- Collection-Daten
- Media-Daten
- technische und normalisierte Metadaten

## API-Anforderungen an die Wallet-Anwendung selbst

Die Wallet-Anwendung sollte intern einen eigenen Wallet-Aggregations-Endpunkt bereitstellen.

Empfohlenes Beispiel:

- `GET /api/wallet/:address/nfts`

Verhalten dieses Endpunkts:

1. Wallet-Holdings extern laden
2. Identity-Liste normieren
3. fehlende NFTs an die Data Platform zur Discovery senden
4. bekannte NFTs aus der Data Platform lesen
5. ein einheitliches Frontend-Response-Modell zurueckgeben

Empfohlene Response-Struktur:

```json
{
  "walletAddress": "0x1234...",
  "items": [
    {
      "chainId": 11155111,
      "contractAddress": "0xa7c41cea4f9195eebdc85054e6b0e799035bf02f",
      "tokenId": "4200042",
      "discoveryStatus": "ready",
      "token": {},
      "collection": {}
    }
  ]
}
```

## Nicht-funktionale Anforderungen

### Sicherheit

- HMAC-Secrets fuer die Data Platform duerfen nur serverseitig gespeichert werden.
- API-Keys fuer Ownership-Provider duerfen nur serverseitig gespeichert werden.
- Browser-Clients duerfen keine internen Secrets sehen.

### Performance

- Wiederholte Wallet-Abfragen sollen bekannte NFTs bevorzugt direkt aus der Data Platform oder aus einem Wallet-eigenen Cache liefern.
- Die Wallet-Anwendung soll Discovery nicht bei jedem Page-Load unnoetig neu anstossen.

### Fehlertoleranz

- Teilfehler einzelner NFTs duerfen nicht die gesamte Wallet-Antwort blockieren.
- Externe Ownership-API-Ausfaelle und Data-Platform-Ausfaelle muessen getrennt behandelt werden.

### Beobachtbarkeit

- Es soll nachvollziehbar sein, fuer welche Wallet welche NFTs entdeckt wurden.
- Discovery-Fehler und Read-Fehler sollen getrennt geloggt werden.

## MVP-Umfang

Ein MVP der Wallet-Anwendung muss mindestens leisten:

1. Wallet verbinden.
2. Extern alle gehaltenen NFTs laden.
3. Aus jeder NFT nur `chainId`, `contractAddress`, `tokenId` normieren.
4. Fuer unbekannte NFTs `POST /api/v1/refresh/token` an die Data Platform senden.
5. Die angereicherten Daten ueber die Data Platform lesen.
6. Im Frontend einen stabilen Status pro NFT darstellen.

## Bewusste Abgrenzung

Nicht Teil dieser Wallet-Anwendung im ersten Schritt:

- eigene vollstaendige NFT-Indexierung ueber alle Contracts und Chains
- direkte Metadatenverarbeitung im Wallet-Frontend
- direkte Live-Chain-Reads fuer NFT-Details im Request-Pfad des Frontends

## Konkrete Empfehlung fuer die Umsetzung

Die Wallet-Anwendung sollte als serverseitig gestuetzte Web-App gebaut werden.

Empfohlenes Setup:

- Frontend fuer Wallet-Login und Rendering
- serverseitiger BFF-Layer fuer Ownership-API und Data-Platform-API
- interne Discovery-Orchestrierung mit limitierter Parallelitaet
- Data Platform als einziges System fuer angereicherte NFT-Daten

Kurz gesagt:

- externe API bestimmt, was die Wallet haelt
- die Data Platform bestimmt, wie diese NFTs angereichert und ausgeliefert werden
- das Wallet-Frontend rendert nur den aggregierten Endzustand