package main

import (
	"github.com/telmocbarros/data-pulse/config"
	"github.com/telmocbarros/data-pulse/internal/handler"
	"fmt"
	"log"
	"net/http"
)

func main() {
	if err := config.SetupDatabase(); err != nil {
		log.Fatal("Error setting up the database. Shutting down the server!")
	}
	defer config.Storage.Close()

	http.HandleFunc("/health", health)
	http.HandleFunc("/dataset", handler.FileUploadHandler)
	fmt.Println("Listening on PORT 8080 ...")
	http.ListenAndServe(":8080", nil)
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("healthy"))
}
