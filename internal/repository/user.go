package repository

import (
	"context"
	"database/sql"
	"errors"
	"myproject/internal/model"
	"time"
)

var (
	// ErrNotFound is returned when no row is found for the given id.
	ErrNotFound = errors.New("repository: user not found")
)

// UserRepo handles all database access for users (pure CRUD).
// It does not contain business logic; that lives in the service layer.
type UserRepo struct {
	db *sql.DB
}

// NewUserRepo creates a new UserRepo with the given database connection.
// db may be nil for testing; in that case all methods return empty or ErrNotFound.
func NewUserRepo(db *sql.DB) *UserRepo {
	return &UserRepo{db: db}
}

// List returns all users from the database, ordered by id ascending.
func (r *UserRepo) List(ctx context.Context) ([]*model.User, error) {
	if r.db == nil {
		return []*model.User{}, nil
	}
	query := `SELECT id, name, email, active, created_at, updated_at FROM users ORDER BY id ASC`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var users []*model.User
	for rows.Next() {
		var u model.User
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.Active, &u.CreatedAt, &u.UpdatedAt); err != nil {
			return nil, err
		}
		users = append(users, &u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if users == nil {
		users = []*model.User{}
	}
	return users, nil
}

// GetByID returns a single user by id, or ErrNotFound if not found.
func (r *UserRepo) GetByID(ctx context.Context, id int64) (*model.User, error) {
	if r.db == nil {
		return nil, ErrNotFound
	}
	query := `SELECT id, name, email, active, created_at, updated_at FROM users WHERE id = $1`
	var u model.User
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&u.ID, &u.Name, &u.Email, &u.Active, &u.CreatedAt, &u.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}

// Create inserts a new user and returns it with id and timestamps set.
func (r *UserRepo) Create(ctx context.Context, name, email string, active bool) (*model.User, error) {
	if r.db == nil {
		now := time.Now()
		return &model.User{ID: 0, Name: name, Email: email, Active: active, CreatedAt: now, UpdatedAt: now}, nil
	}
	query := `INSERT INTO users (name, email, active, created_at, updated_at)
	         VALUES ($1, $2, $3, $4, $5)
	         RETURNING id, name, email, active, created_at, updated_at`
	now := time.Now()
	var u model.User
	err := r.db.QueryRowContext(ctx, query, name, email, active, now, now).Scan(
		&u.ID, &u.Name, &u.Email, &u.Active, &u.CreatedAt, &u.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// Update updates an existing user by id. Returns the updated user or ErrNotFound.
func (r *UserRepo) Update(ctx context.Context, id int64, name, email *string, active *bool) (*model.User, error) {
	if r.db == nil {
		return nil, ErrNotFound
	}
	// In a full implementation you might use dynamic SQL or separate UPDATE columns.
	// Here we assume we always update name, email, active and updated_at.
	query := `UPDATE users SET name = COALESCE($2, name), email = COALESCE($3, email), active = COALESCE($4, active), updated_at = $5
	         WHERE id = $1
	         RETURNING id, name, email, active, created_at, updated_at`
	now := time.Now()
	var u model.User
	err := r.db.QueryRowContext(ctx, query, id, name, email, active, now).Scan(
		&u.ID, &u.Name, &u.Email, &u.Active, &u.CreatedAt, &u.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &u, nil
}

// Delete removes a user by id. Returns ErrNotFound if the user does not exist.
func (r *UserRepo) Delete(ctx context.Context, id int64) error {
	if r.db == nil {
		return ErrNotFound
	}
	query := `DELETE FROM users WHERE id = $1`
	result, err := r.db.ExecContext(ctx, query, id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
