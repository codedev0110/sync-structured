package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"myproject/internal/model"
	"myproject/internal/service"
)

// UserHandler receives HTTP requests and delegates to the service layer.
// It is responsible only for parsing request and writing response.
type UserHandler struct {
	svc *service.UserService
}

// NewUserHandler creates a new UserHandler with the given service.
func NewUserHandler(svc *service.UserService) *UserHandler {
	return &UserHandler{svc: svc}
}

// Register registers all user routes on mux: GET/POST /users, GET/PUT/DELETE /users/:id.
func (h *UserHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/users", h.handleUsers)
	mux.HandleFunc("/users/", h.handleUserByID)
}

// handleUsers handles GET /users (list) and POST /users (create).
func (h *UserHandler) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.List(w, r)
		return
	case http.MethodPost:
		h.Create(w, r)
		return
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
}

// handleUserByID handles GET /users/:id, PUT /users/:id, DELETE /users/:id.
func (h *UserHandler) handleUserByID(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDFromPath(r.URL.Path, "/users/")
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid user id")
		return
	}
	switch r.Method {
	case http.MethodGet:
		h.Get(w, r, id)
		return
	case http.MethodPut:
		h.Update(w, r, id)
		return
	case http.MethodDelete:
		h.Delete(w, r, id)
		return
	default:
		respondError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
}

// List handles GET /users — returns all users as JSON.
func (h *UserHandler) List(w http.ResponseWriter, r *http.Request) {
	users, err := h.svc.List(r.Context())
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if users == nil {
		users = []*model.User{}
	}
	respondJSON(w, http.StatusOK, users)
}

// Get handles GET /users/:id — returns one user or 404.
func (h *UserHandler) Get(w http.ResponseWriter, r *http.Request, id int64) {
	user, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			respondError(w, http.StatusNotFound, "user not found")
			return
		}
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, user)
}

// Create handles POST /users — creates a user from JSON body.
func (h *UserHandler) Create(w http.ResponseWriter, r *http.Request) {
	var in model.CreateUserInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	user, err := h.svc.Create(r.Context(), &in)
	if err != nil {
		var valErr model.ErrValidation
		if errors.As(err, &valErr) {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, user)
}

// Update handles PUT /users/:id — updates a user from JSON body.
func (h *UserHandler) Update(w http.ResponseWriter, r *http.Request, id int64) {
	var in model.UpdateUserInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	user, err := h.svc.Update(r.Context(), id, &in)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			respondError(w, http.StatusNotFound, "user not found")
			return
		}
		var valErr model.ErrValidation
		if errors.As(err, &valErr) {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, user)
}

// Delete handles DELETE /users/:id — deletes a user and returns 204.
func (h *UserHandler) Delete(w http.ResponseWriter, r *http.Request, id int64) {
	err := h.svc.Delete(r.Context(), id)
	if err != nil {
		if errors.Is(err, model.ErrNotFound) {
			respondError(w, http.StatusNotFound, "user not found")
			return
		}
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseIDFromPath extracts an int64 id from path like "/users/123".
func parseIDFromPath(path, prefix string) (int64, error) {
	s := strings.TrimPrefix(path, prefix)
	s = strings.Trim(s, "/")
	if s == "" {
		return 0, errors.New("missing id")
	}
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id < 1 {
		return 0, errors.New("invalid id")
	}
	return id, nil
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
