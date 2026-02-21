// Package main is the entry point for the application.
// It wires Handler → Service → Repository → DB and starts the HTTP server.
package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"myproject/internal/handler"
	"myproject/internal/repository"
	"myproject/internal/service"
)

func main() {
	// Database: use real connection in production, e.g.:
	//   dsn := os.Getenv("DATABASE_DSN")
	//   db, err := sql.Open("postgres", dsn)
	var db *sql.DB
	// db, err := sql.Open("postgres", "postgres://user:pass@localhost/dbname?sslmode=disable")
	// if err != nil { log.Fatal(err) }
	// defer db.Close()
	// if err := db.Ping(); err != nil { log.Fatal(err) }

	repo := repository.NewUserRepo(db)
	svc := service.NewUserService(repo)
	userHandler := handler.NewUserHandler(svc)

	mux := http.NewServeMux()
	userHandler.Register(mux)

	// Optional: health check and root
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"service":"myapp","version":"1.0"}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("server stopped")
}
