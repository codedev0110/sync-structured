package service

import (
	"myproject/internal/model"
	"myproject/internal/repository"
)

// UserService contains business logic for users.
type UserService struct {
	repo *repository.UserRepo
}

// NewUserService creates a new UserService.
func NewUserService(repo *repository.UserRepo) *UserService {
	return &UserService{repo: repo}
}

// List returns all users (business rules applied here).
func (s *UserService) List() ([]*model.User, error) {
	return s.repo.List()
}
