package main

import (
	"database/sql"
	"log"
	"net/http"

	"myproject/internal/handler"
	"myproject/internal/repository"
	"myproject/internal/service"
)

func main() {
	var db *sql.DB
	// db, err := sql.Open("driver", "dsn") — real DB when needed

	repo := repository.NewUserRepo(db)
	svc := service.NewUserService(repo)
	h := handler.NewUserHandler(svc)

	http.HandleFunc("/users", h.List)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
