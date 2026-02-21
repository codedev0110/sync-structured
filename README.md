# myproject

Standard Go project structure: **Handler → Service → Repository → DB**. Dependencies point inward; no circular deps. Test-friendly: mock repository in service tests, mock service in handler tests.

## Structure

```
myproject/
├── cmd/
│   ├── myapp/           # HTTP server (User API)
│   │   └── main.go      # Entry point: repo → svc → handler, /users, ListenAndServe
│   └── sync-cli/        # Record sync CLI (period / auto)
│       └── main.go      # Entry point: SyncService, parseArgs, StartRecordProcessing
├── internal/
│   ├── handler/         # HTTP handlers / controllers
│   │   └── user.go      # UserHandler, List (GET /users)
│   ├── service/         # Business logic
│   │   ├── user.go      # UserService, List
│   │   └── sync.go      # SyncService (record sync logic)
│   ├── repository/      # Database access (pure CRUD)
│   │   ├── db.go        # DB interface (records/streams)
│   │   ├── user.go      # UserRepo, List
│   │   └── record.go    # InsertRecord, UpdateRecordNotApproved, DisableResults
│   ├── model/           # Data structures (no DB/HTTP logic)
│   │   ├── user.go      # User
│   │   ├── record.go    # Record
│   │   ├── stream.go    # Stream
│   │   └── period.go    # Period
│   └── utils/
│       └── interface.go # Utils interface (sync CLI)
├── pkg/
│   └── utils/           # Reusable public packages
├── api/                 # API specs (OpenAPI, proto)
├── configs/             # Config files
├── migrations/          # DB migrations
├── go.mod
├── go.sum
└── README.md
```

## Build

```bash
# HTTP server (User API)
go build -o myapp ./cmd/myapp

# Record sync CLI
go build -o sync-cli ./cmd/sync-cli
```

## Run

```bash
# Start HTTP server on :8080
./myapp
# GET http://localhost:8080/users

# Run sync CLI (period or auto)
./sync-cli period -start "2025-01-01 00:00" -end "2025-01-02 00:00" -stream_type audio
./sync-cli auto -days 2 -stream_type audio --sync
```

## Flow

- **User API:** Handler → Service → Repository → DB (minimal `main.go` in `cmd/myapp`).
- **Sync CLI:** `cmd/sync-cli` uses SyncService (internal/service/sync.go) and repository/model; no HTTP.
