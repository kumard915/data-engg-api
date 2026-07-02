# mock-data-generator-api-v3

Mock fintech data generator (REST API) with file output and optional Kafka producer.
Generates realistic merchants, accounts, payins and payouts data and can stream events to Kafka topics.

## Quick start (recommended)

Requirements:
- Node.js 18+ and npm
- Docker & Docker Compose (for local Kafka)

1. Unzip and cd into project:
```bash
cd mock-data-generator-api-v3
```

2. Copy .env example and edit if needed:
```bash
cp .env.example .env
```

3. Start Kafka & Zookeeper via Docker Compose (optional, required for streaming):
```bash
docker-compose up -d
```

4. Install Node dependencies:
```bash
npm install
```

5. Start the server (development, nodemon):
```bash
npm run dev
```

6. Try endpoints:
- Health check: `GET http://localhost:4000/health`
- Generate 100 payins (no Kafka): `GET http://localhost:4000/generate/payin?count=100`
- Generate 100 payins and stream to Kafka: `GET http://localhost:4000/generate/payin?count=100&stream=true&extended=true`
- Generate today's files and optionally stream: `GET http://localhost:4000/generate/all?stream=true`
- Generate historical data: `GET http://localhost:4000/generate/history?from=2025-08-20&to=2025-10-09&stream=true`

## Kafka topics created/used:
- payin-events
- payout-events

If Kafka is not available the server will continue to write files and log a warning (no crash).

## Notes
- Files are written under `./data/YYYY-MM-DD/` as JSON and CSV.
- Extended schema is enabled with `extended=true` query parameter.
- `stream=true` enables Kafka publishing for the generated records.

