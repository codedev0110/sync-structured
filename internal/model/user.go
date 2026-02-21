package model

import "time"

// User represents the domain entity stored in the database.
// All fields map to the "users" table.
type User struct {
	ID        int64     `json:"id" db:"id"`
	Name      string    `json:"name" db:"name"`
	Email     string    `json:"email" db:"email"`
	Active    bool      `json:"active" db:"active"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// CreateUserInput is the payload for creating a new user.
// Used in POST /users request body.
type CreateUserInput struct {
	Name   string `json:"name"`
	Email  string `json:"email"`
	Active *bool  `json:"active,omitempty"`
}

// UpdateUserInput is the payload for updating an existing user.
// All fields are optional; only non-zero values are applied.
// Used in PUT /users/:id request body.
type UpdateUserInput struct {
	Name   *string `json:"name,omitempty"`
	Email  *string `json:"email,omitempty"`
	Active *bool   `json:"active,omitempty"`
}

// Validate checks CreateUserInput for business rules.
// Returns a non-nil error if name or email is empty.
func (c *CreateUserInput) Validate() error {
	if c.Name == "" {
		return ErrValidation("name is required")
	}
	if c.Email == "" {
		return ErrValidation("email is required")
	}
	return nil
}
