package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/handler"
	"github.com/telmocbarros/data-pulse/internal/service/jobmanager"
)

func main() {
	if err := config.SetupDatabase(); err != nil {
		log.Fatal("Error setting up the database. Shutting down the server!")
	}
	defer config.Storage.Close()

	if err := config.SetupFileStorage(); err != nil {
		log.Fatal("Error setting up file storage. Shutting down the server!")
	}

	jobmanager.Init(4)

	http.HandleFunc("/health", health)

	http.HandleFunc("/api/datasets/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/api/datasets/")

		// POST /api/datasets/ — upload a new dataset
		if path == "" && r.Method == http.MethodPost {
			handler.FileUploadHandler(w, r)
			return
		}

		// /api/datasets/{id}/profile
		if strings.HasSuffix(path, "/profile") {
			switch r.Method {
			case http.MethodGet:
				handler.GetProfileHandler(w, r)
			case http.MethodPost:
				handler.CreateProfileHandler(w, r)
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			}
			return
		}

		http.Error(w, "Not found", http.StatusNotFound)
	})

	http.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cancel") && r.Method == http.MethodPost {
			handler.CancelJobHandler(w, r)
			return
		}
		if r.Method == http.MethodGet {
			handler.GetJobHandler(w, r)
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	})

	fmt.Println("Listening on PORT 8080 ...")
	http.ListenAndServe(":8080", nil)
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("healthy"))
}
