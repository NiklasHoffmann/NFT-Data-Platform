# Anforderungen fuer einen Admin- und Governance-Service

Dieses Dokument beschreibt die Zielarchitektur und die Anforderungen fuer einen separaten Admin- und Governance-Service, der parallel zur NFT Data Platform, zum Wallet-Service und zur IdeationMarket-Anwendung entwickelt wird.

Die NFT Data Platform selbst ist dabei nicht der Ort fuer Marketplace-Steuerung. Das vorhandene Web-Frontend in diesem Repository dient nur als Test-, Operator- und Integrationsoberflaeche fuer API und Worker.

## Zielbild

Der Admin- und Governance-Service ist ein eigenes Backend-Microservice mit eigener UI oder eigenem Admin-Frontend.

Er ist verantwortlich fuer die operative und regelbasierte Steuerung des Marktplatzes.

Typische Verantwortlichkeiten:

- Collection-Whitelist verwalten
- Buyer-Whitelist verwalten
- erlaubte Payment-Currencies verwalten
- Marketplace pausieren oder entsperren
- Governance-Aenderungen ueber eine Multisig-Wallet vorbereiten und ausloesen
- Konfigurationen und Policies im Marktplatz nachvollziehbar machen

## Service-Grenzen

### Quelle der Wahrheit fuer Marketplace-Regeln

Die Quelle der Wahrheit fuer Marketplace-Regeln bleibt die IdeationMarket-Anwendung und der zugrunde liegende Contract.

Der Admin- und Governance-Service steuert diese Regeln, aber er ersetzt nicht die Vertragslogik.

### Rolle der NFT Data Platform

Die NFT Data Platform bleibt fuer diesen Service optional und read-only.

Sie kann genutzt werden fuer:

- NFT-Metadaten
- Collection-Metadaten
- angereicherte Token- und Collection-Ansichten

Sie ist nicht verantwortlich fuer:

- Listing-Erstellung
- Kaufabwicklung
- Cancel/Update-Flow von Listings
- Buyer-Whitelist-Entscheidungen
- Collection-Whitelist-Entscheidungen
- Multisig-Governance-Entscheidungen

## Kommunikationsbeziehungen

Der Admin- und Governance-Service kommuniziert primaer mit der IdeationMarket-Anwendung oder direkt mit dem Marketplace-Contract-Kontext.

Er kommuniziert nur optional mit der NFT Data Platform, wenn fuer Admin-Ansichten angereicherte NFT-Daten gebraucht werden.

Empfohlene Kommunikationsrichtung:

1. Admin-Governance-Service liest Marketplace-Zustand aus der IdeationMarket-Domaene.
2. Admin-Governance-Service fuehrt Governance-Operationen ueber Multisig oder signierte Admin-Flows aus.
3. Marktplatz und Wallet konsumieren die daraus resultierenden Marketplace-Regeln.
4. NFT Data Platform bleibt davon entkoppelt und liefert nur NFT-Daten, wenn benoetigt.

## Fachliche Anforderungen

### 1. Multisig-Integration

Der Service muss mit einer Multisig-Wallet oder einem Multisig-gesteuerten Governance-Flow arbeiten koennen.

Mindestanforderungen:

- Governance-Transaktionen vorbereiten
- Transaktionsdaten nachvollziehbar anzeigen
- Status offener Governance-Aktionen darstellen
- Ausgefuehrte Governance-Aenderungen historisieren

### 2. Marketplace-Policy-Verwaltung

Der Service muss mindestens folgende Marktregeln steuern oder darstellen koennen:

- Collection-Whitelist
- Buyer-Whitelist
- Allowed Buyers fuer einzelne Listings
- erlaubte Payment-Currencies
- Innovation Fee oder aehnliche Fee-Konfiguration
- Pause-Status des Marktplatzes

### 3. Marketplace-Contract-Kompatibilitaet

Die Service-Architektur muss auf die IdeationMarket-ABI abgestimmt sein.

Relevante Signale aus der ABI sind unter anderem:

- `ListingCreated`
- `ListingUpdated`
- `ListingPurchased`
- `ListingCanceled`
- `CollectionWhitelistRevokedCancelTriggered`
- `InnovationFeeUpdated`
- Fehler wie `IdeationMarket__BuyerNotWhitelisted`, `IdeationMarket__CollectionNotWhitelisted` und `IdeationMarket__ContractPaused`

### 4. Read-Model fuer Governance und Admin

Der Admin- und Governance-Service sollte ein eigenes Read-Model besitzen.

Typische Sichten:

- aktuelle Marketplace-Konfiguration
- Collection-Whitelist-Status
- Listing-Whitelist-Status
- Governance-Aktionshistorie
- ausstehende Multisig-Aktionen
- Audit-Trail fuer Admin-Eingriffe

### 5. Sicherheitsanforderungen

Der Service ist sicherheitskritisch und braucht strengere Anforderungen als Wallet oder Marketplace-Frontend.

Mindestanforderungen:

- starke Authentifizierung fuer Admin-Zugriffe
- klare Rollentrennung
- Audit Logging
- keine direkten Secrets im Browser
- klare Trennung zwischen Leserechten und steuernden Operationen

## Empfohlene Architektur

Es gibt kuenftig vier getrennte Services:

1. NFT Data Platform fuer NFT-Discovery und Enrichment
2. Wallet-Service fuer Portfolio- und Wallet-Flows
3. IdeationMarket-Service fuer Marketplace- und Listing-Logik
4. Admin-Governance-Service fuer Marktplatzsteuerung und Governance

Empfohlene Verantwortlichkeiten:

- Wallet-Service: Wallet-Login, Holdings, Portfolio-Sicht
- IdeationMarket-Service: Listing-, Kauf-, Verkaufs- und Handelslogik
- Admin-Governance-Service: Policies, Whitelists, Pause, Fees, Governance
- NFT Data Platform: NFT-Metadaten, Collection-Daten, Discovery und API-Auslieferung

## Integrationsprinzip

Wenn der Admin-Governance-Service NFT-Kontext braucht, soll er ihn nur lesen.

Empfehlung:

- Marketplace-Regeln immer aus IdeationMarket oder dessen Governance-Kontext lesen oder schreiben
- NFT-Kontext bei Bedarf aus der NFT Data Platform lesen
- keine Marketplace-Steuerlogik in die Data Platform verschieben

## MVP-Umfang

Ein MVP des Admin- und Governance-Service muss mindestens leisten:

1. aktuelle Marketplace-Regeln sichtbar machen
2. Whitelist- und Policy-Aenderungen vorbereiten
3. Multisig-Aktionen nachvollziehbar darstellen
4. sicherstellen, dass Governance-Aenderungen gegen den Marktplatz korrekt ausgefuehrt werden
5. optional NFT- und Collection-Kontext aus der Data Platform einblenden

## Kurzfassung

- die Data Platform bleibt NFT-Daten-Service
- IdeationMarket bleibt Source of Truth fuer Marketplace-Regeln
- der neue Admin-Governance-Service steuert den Marktplatz und kommuniziert dafuer mit IdeationMarket
- Wallet, Marketplace und Admin-Governance bleiben getrennte Services