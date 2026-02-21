package repository

import "myproject/internal/model"

// DB is the database interface used for local and remote servers.
// Concrete implementation (e.g. PostgreSQL) is provided by the caller.
type DB interface {
	SelectRecords(query string) ([]model.Record, error)
	SelectStreams(query string) ([]model.Stream, error)
	Insert(query string, args ...interface{}) (int64, error)
	Update(query string, args ...interface{}) error
}
