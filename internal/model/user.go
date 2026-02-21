package model

// User represents the domain entity (no DB or HTTP logic).
type User struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}
