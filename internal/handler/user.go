package handler

import (
	"encoding/json"
	"net/http"

	"myproject/internal/model"
	"myproject/internal/service"
)

// UserHandler receives HTTP requests and delegates to the service.
type UserHandler struct {
	svc *service.UserService
}

// NewUserHandler creates a new UserHandler.
func NewUserHandler(svc *service.UserService) *UserHandler {
	return &UserHandler{svc: svc}
}

// List handles GET /users — calls service and returns JSON.
func (h *UserHandler) List(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	users, err := h.svc.List()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if users == nil {
		users = []*model.User{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(users)
}
