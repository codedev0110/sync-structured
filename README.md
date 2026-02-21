# myproject

Standard Go project layout: **Handler → Service → Repository → DB**. Dependencies point inward; testing is easy (mock repository in service tests, mock service in handler tests).

## Structure

```
myproject/
├── cmd/
│   └── myapp/
│       └── main.go              # Entry point: wiring and HTTP server
├── internal/
│   ├── handler/                 # HTTP handlers / controllers
│   │   └── user.go              # REST: List, Get, Create, Update, Delete
│   ├── service/                 # Business logic
│   │   └── user.go              # Validation, rules, calls repository
│   ├── repository/              # Database access (pure CRUD)
│   │   └── user.go              # List, GetByID, Create, Update, Delete
│   └── model/                   # Data structures
│       ├── user.go              # User, CreateUserInput, UpdateUserInput
│       └── errors.go            # ErrValidation, ErrNotFound
├── pkg/
│   └── utils/
│       └── utils.go             # StringPtr, Int64Ptr, ParseInt64, etc.
├── api/
│   └── openapi.yaml             # OpenAPI 3 spec for /users
├── configs/
│   └── config.example.yaml      # Example server and DB config
├── migrations/
│   ├── 001_create_users.up.sql  # Create users table
│   └── 001_create_users.down.sql
├── go.mod
├── go.sum
└── README.md
```

## Run

```bash
go run ./cmd/myapp
```

Server listens on `:8080` (or set `PORT` env). Graceful shutdown on SIGINT/SIGTERM.

## Build

```bash
go build -o myapp ./cmd/myapp
./myapp
```

## API (REST)

| Method   | Path      | Description        |
|----------|-----------|--------------------|
| GET      | /users    | List all users     |
| GET      | /users/:id| Get user by ID     |
| POST     | /users    | Create user (JSON) |
| PUT      | /users/:id| Update user (JSON) |
| DELETE   | /users/:id| Delete user        |

- **GET /** — service info JSON  
- **GET /health** — health check (200 OK)

### Examples

```bash
# List users
curl http://localhost:8080/users

# Create user
curl -X POST http://localhost:8080/users -H "Content-Type: application/json" -d '{"name":"Alice","email":"alice@example.com"}'

# Get user
curl http://localhost:8080/users/1

# Update user
curl -X PUT http://localhost:8080/users/1 -H "Content-Type: application/json" -d '{"name":"Alice Updated"}'

# Delete user
curl -X DELETE http://localhost:8080/users/1
```

## Config

Copy `configs/config.example.yaml` to `configs/config.yaml` and set `database.dsn` and other values. The app currently reads `PORT` from the environment (default 8080); full config loading can be added in `main.go`.

## Migrations

Migrations are in `migrations/` (PostgreSQL). Apply with your preferred tool, e.g.:

```bash
migrate -path migrations -database "postgres://user:pass@localhost/db?sslmode=disable" up
```

Without a real DB, the app still runs: repository returns empty list and 404 for Get/Update/Delete when `db` is nil.
