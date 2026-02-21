package service

import (
	"context"
	"errors"
	"myproject/internal/model"
	"myproject/internal/repository"
	"strings"
)

// UserService contains all business logic for users.
// It calls the repository for persistence and applies validation and rules.
type UserService struct {
	repo *repository.UserRepo
}

// NewUserService creates a new UserService with the given repository.
func NewUserService(repo *repository.UserRepo) *UserService {
	return &UserService{repo: repo}
}

// List returns all users. Business rules (e.g. filtering inactive) can be applied here.
func (s *UserService) List(ctx context.Context) ([]*model.User, error) {
	return s.repo.List(ctx)
}

// GetByID returns a user by id. Returns model.ErrNotFound if not found.
func (s *UserService) GetByID(ctx context.Context, id int64) (*model.User, error) {
	u, err := s.repo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	return u, nil
}

// Create creates a new user after validating input.
func (s *UserService) Create(ctx context.Context, in *model.CreateUserInput) (*model.User, error) {
	if in == nil {
		return nil, model.ErrValidation("request body is required")
	}
	if err := in.Validate(); err != nil {
		return nil, err
	}
	active := true
	if in.Active != nil {
		active = *in.Active
	}
	name := strings.TrimSpace(in.Name)
	email := strings.TrimSpace(in.Email)
	return s.repo.Create(ctx, name, email, active)
}

// Update updates an existing user. Only non-nil fields in in are applied.
func (s *UserService) Update(ctx context.Context, id int64, in *model.UpdateUserInput) (*model.User, error) {
	if in == nil {
		return s.GetByID(ctx, id)
	}
	var name, email *string
	var active *bool
	if in.Name != nil {
		n := strings.TrimSpace(*in.Name)
		if n == "" {
			return nil, model.ErrValidation("name cannot be empty")
		}
		name = &n
	}
	if in.Email != nil {
		e := strings.TrimSpace(*in.Email)
		if e == "" {
			return nil, model.ErrValidation("email cannot be empty")
		}
		email = &e
	}
	if in.Active != nil {
		active = in.Active
	}
	u, err := s.repo.Update(ctx, id, name, email, active)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	return u, nil
}

// Delete removes a user by id. Returns model.ErrNotFound if not found.
func (s *UserService) Delete(ctx context.Context, id int64) error {
	err := s.repo.Delete(ctx, id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return model.ErrNotFound
		}
		return err
	}
	return nil
}
