# Anforderungen fuer ein Marktplatz-Frontend mit NFT Data Platform

Dieses Dokument beschreibt die Anforderungen fuer ein Marktplatz-Frontend, das parallel zur NFT Data Platform aufgebaut wird.

Die NFT Data Platform selbst ist dabei nicht das spaetere Marktplatz-Frontend. Das vorhandene Web-Frontend in diesem Repository dient nur als Test-, Operator- und Integrationsoberflaeche fuer API und Worker.

Neben dem Marktplatz-Service wird in der Zielarchitektur ein separater Admin- und Governance-Service betrieben, der die Marketplace-Regeln steuert und dazu mit IdeationMarket kommuniziert.

Das Zielbild ist:

- TheGraph oder eine vergleichbare Query-Quelle liefert die relevanten NFT-Identitaeten fuer den Marktplatz
- die NFT Data Platform uebernimmt Discovery, Enrichment und Read-API
- das Marktplatz-Frontend rendert die Daten aus der Data Platform statt direkt aus TheGraph-Metadaten

## Zielarchitektur

Das Marktplatz-System nutzt zwei klar getrennte Datenquellen.

Quelle 1: TheGraph oder eine vergleichbare Marketplace-/Indexing-Quelle

- Welche NFTs sollen im Marktplatz angezeigt werden?
- Welche Listings, Offers oder Collection-Items sind relevant?
- Rueckgabe mindestens von `chainId`, `contractAddress`, `tokenId`

Quelle 2: NFT Data Platform

- Token-Metadaten
- Collection-Daten
- Media-Daten
- normalisierte NFT-Darstellung fuer das Frontend

Das Marktplatz-Frontend, die NFT Data Platform und der separate Admin- und Governance-Service werden als getrennte Services entwickelt und deployed.

## Architekturprinzip

Das Marktplatz-Frontend darf die anzuzeigenden NFTs aus TheGraph ableiten, aber nicht die endgueltige NFT-Darstellung aus TheGraph rendern.

Die Data Platform wird in dieser Architektur als eigenes Backend-Microservice behandelt, nicht als Teil des Marktplatz-Frontends.

Die steuernde Marketplace-Governance liegt ebenfalls nicht in der Data Platform, sondern in einem separaten Admin- und Governance-Service zusammen mit der IdeationMarket-Domaene.

Stattdessen gilt dieser Ablauf:

1. Das Marktplatz-Backend oder ein serverseitiger BFF fragt TheGraph ab.
2. TheGraph liefert die Liste der relevanten NFTs fuer den Marktplatz.
3. Das Marktplatz-Backend normiert diese Liste auf `chainId`, `contractAddress`, `tokenId`.
4. Das Marktplatz-Backend sendet diese Liste an die NFT Data Platform.
5. Die Data Platform liefert bereits bekannte NFTs sofort zurueck.
6. Fehlende NFTs werden automatisch zur Discovery an den Worker gegeben.
7. Das Marktplatz-Frontend liest anschliessend die angereicherten NFT-Daten aus der Data Platform.

## Fachliche Anforderungen

### 1. Externe Markt-/Listing-Abfrage

Das Marktplatz-System muss TheGraph oder eine vergleichbare Quelle nutzen, um die NFTs zu bestimmen, die im Marktplatz angezeigt werden sollen.

Mindestdaten aus dieser Quelle:

- `chainId`
- `contractAddress`
- `tokenId`

Optional zusaetzlich sinnvoll:

- `listingId`
- `sellerAddress`
- `price`
- `currency`
- `marketStatus`

Diese Felder duerfen fuer Listing- oder Handelslogik genutzt werden. NFT-Metadaten und Collection-Daten sollen aber aus der Data Platform kommen.

### 2. Normiertes Token-Identity-Format

Alle NFT-Identitaeten muessen intern so weitergegeben werden:

```json
{
  "chainId": 11155111,
  "contractAddress": "0xa7c41cea4f9195eebdc85054e6b0e799035bf02f",
  "tokenId": "4200042"
}
```

Anforderungen:

- `contractAddress` als normalisierte EVM-Adresse
- `tokenId` immer als String
- `chainId` als positive Integer-ID

### 3. Batch-Discovery ueber die NFT Data Platform

Fuer vom Marktplatz bestimmte NFTs soll die Data Platform ueber einen Batch-Call angesprochen werden.

Der dafuer vorgesehene Endpunkt ist:

- `POST /api/v1/tokens/discover`

Beispiel-Request:

```json
{
  "items": [
    {
      "chainId": 11155111,
      "contractAddress": "0xa7c41cea4f9195eebdc85054e6b0e799035bf02f",
      "tokenId": "4200042"
    }
  ]
}
```

Erwartetes Verhalten:

- indexierte NFTs kommen sofort mit `discoveryStatus: ready` zurueck
- nicht indexierte NFTs werden zur Discovery queued und kommen mit `discoveryStatus: queued` zurueck

### 4. Read-API fuer Marktplatzkarten und Detailseiten

Fuer die Darstellung im Marktplatz sollen die angereicherten NFT-Daten aus der Data Platform kommen.

Wichtige Endpunkte:

- `POST /api/v1/tokens/discover`
- `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId`
- `GET /api/v1/collections/:chainId/:contractAddress`
- `GET /api/v1/tokens`
- `GET /api/v1/search`

### 5. Zustandsmodell im Marktplatz-Frontend

Das Frontend muss mindestens diese Zustaende pro NFT verarbeiten koennen:

- `ready`
- `queued`
- `failed`
- `partial`

Empfohlene Bedeutung:

- `ready`: NFT-Daten sind sofort voll oder ausreichend verfuegbar
- `queued`: NFT wird im Hintergrund discoverd und sollte spaeter erneut gelesen werden
- `failed`: Discovery war nicht erfolgreich
- `partial`: Token ist bekannt, aber einzelne Daten wie Media fehlen noch

### 6. Render-Verhalten fuer den Marktplatz

Das Marktplatz-Frontend soll vorhandene NFTs sofort anzeigen koennen und fehlende nachladen.

Das bedeutet konkret:

- `ready`-Items direkt rendern
- `queued`-Items mit Placeholder oder Skeleton rendern
- nach einem kurzen Delay erneut synchronisieren oder gezielt Details nachladen

### 7. Polling- und Retry-Strategie

Nach einem `tokens/discover`-Call muss das Marktplatz-System fuer `queued`-Items erneut lesen koennen.

Mindestanforderungen:

- kurzes Polling fuer frisch angefragte NFTs
- Backoff bei laenger laufenden Jobs
- keine unendliche Hochfrequenz-Abfrage

### 8. BFF-/Backend-Anforderung

Das Marktplatz-Frontend soll nicht direkt mit TheGraph und der HMAC-geschuetzten Data-Platform-API sprechen.

Ein serverseitiger BFF oder Marktplatz-Backend-Layer soll:

- TheGraph abfragen
- die Token-Identitaeten normieren
- `POST /api/v1/tokens/discover` aufrufen
- die Antwort fuer das Frontend aggregieren

## Empfohlene interne Marktplatz-API

Das Marktplatz-System sollte einen internen Aggregations-Endpunkt bereitstellen.

Beispiel:

- `GET /api/marketplace/nfts`

Verhalten:

1. Listings oder Markt-NFTs aus TheGraph laden
2. Token-Identitaeten extrahieren
3. die Liste an die Data Platform senden
4. `ready`-Items sofort zurueckgeben
5. `queued`-Items mit Status zurueckgeben

## Beispiel fuer eine Aggregat-Response

```json
{
  "items": [
    {
      "listingId": "listing-123",
      "chainId": 11155111,
      "contractAddress": "0xa7c41cea4f9195eebdc85054e6b0e799035bf02f",
      "tokenId": "4200042",
      "discoveryStatus": "ready",
      "token": {},
      "collection": {},
      "market": {
        "price": "0.15",
        "currency": "ETH"
      }
    }
  ]
}
```

## Nicht-funktionale Anforderungen

### Sicherheit

- HMAC-Secrets fuer die Data Platform duerfen nur serverseitig gespeichert werden
- TheGraph-Zugriffe sollen serverseitig gebuendelt werden, wenn dort private Query-Keys genutzt werden

### Performance

- bereits bekannte NFTs sollen ohne erneute Voll-Discovery direkt aus der Data Platform kommen
- Batch-Requests sollen dedupliziert werden

### Fehlertoleranz

- einzelne kaputte NFTs duerfen nicht die gesamte Marktplatzseite blockieren
- TheGraph-Fehler und Data-Platform-Fehler muessen getrennt behandelt werden

## MVP-Umfang

Ein MVP des Marktplatz-Frontends muss mindestens leisten:

1. relevante NFT-Identitaeten aus TheGraph laden
2. diese an `POST /api/v1/tokens/discover` schicken
3. `ready`-Items sofort anzeigen
4. `queued`-Items spaeter nachladen und dann anzeigen
5. Collection- und Token-Daten aus der Data Platform rendern

## Kurzfassung

- TheGraph bestimmt, welche NFTs im Marktplatz sichtbar sein sollen
- die Data Platform bestimmt, wie diese NFTs angereichert und ausgeliefert werden
- das Marktplatz-Frontend rendert den aggregierten Zustand aus beiden Systemen