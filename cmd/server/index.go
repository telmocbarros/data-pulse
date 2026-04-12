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

	jobmanager.Init(4)

	http.HandleFunc("/health", health)
	http.HandleFunc("/dataset", handler.FileUploadHandler)
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
