package repository

import (
	"database/sql"
	"myproject/internal/model"
)

// UserRepo talks to the database for users (pure CRUD).
type UserRepo struct {
	db *sql.DB
}

// NewUserRepo creates a new UserRepo.
func NewUserRepo(db *sql.DB) *UserRepo {
	return &UserRepo{db: db}
}

// List returns all users from the database.
func (r *UserRepo) List() ([]*model.User, error) {
	if r.db == nil {
		return []*model.User{}, nil
	}
	// TODO: rows, err := r.db.Query("SELECT id, name FROM users"); ...
	return []*model.User{}, nil
}
