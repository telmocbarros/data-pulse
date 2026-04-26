package main

import (
	"log"
	"net/http"
	"time"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/handler"
	"github.com/telmocbarros/data-pulse/internal/service/jobmanager"
)

func main() {
	if err := config.SetupDatabase(); err != nil {
		log.Fatalf("Error setting up the database: %v", err)
	}
	defer config.Storage.Close()

	if err := config.SetupFileStorage(); err != nil {
		log.Fatalf("Error setting up file storage: %v", err)
	}

	jobmanager.Init(4)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", health)

	mux.HandleFunc("GET /api/datasets/{$}", handler.ListDatasetsHandler)
	mux.HandleFunc("POST /api/datasets/{$}", handler.FileUploadHandler)
	mux.HandleFunc("GET /api/datasets/{id}", handler.GetDatasetHandler)
	mux.HandleFunc("GET /api/datasets/{id}/profile", handler.GetProfileHandler)
	mux.HandleFunc("POST /api/datasets/{id}/profile", handler.CreateProfileHandler)

	mux.HandleFunc("GET /api/jobs/{id}", handler.GetJobHandler)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", handler.CancelJobHandler)

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       5 * time.Minute,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       2 * time.Minute,
	}

	log.Println("Listening on PORT 8080 ...")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("healthy"))
}
