package handler

import (
	"fmt"
	"log"
	"net/http"

	service "github.com/telmocbarros/data-pulse/internal/service/dataset_upload"
)

func FileUploadHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		fmt.Println("Error retrieving multipart form")
		http.Error(w, "Error retrieving multipart form", http.StatusBadRequest)
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println("Error retrieving file: ", err)
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	fmt.Println("content type: ", handler.Header.Get("Content-Type"))

	switch handler.Header.Get("Content-Type") {
	case "application/json":
		record, validationErrors, err := service.ProcessJsonFile(file, handler.Filename)
		if err != nil {
			fmt.Println("Error parsing JSON file: ", err)
			http.Error(w, "Error parsing JSON file", http.StatusInternalServerError)
			return
		}
		if len(validationErrors) > 0 {
			log.Printf("JSON parsed with %d validation errors\n", len(validationErrors))
		}
		fmt.Fprintf(w, "Successfully parsed JSON file: %d rows, %d validation errors\n", len(record), len(validationErrors))

	case "text/csv":
		record, validationErrors, err := service.ProcessCsvFile(file, handler.Filename)
		if err != nil {
			http.Error(w, "Error parsing csv file", http.StatusInternalServerError)
			return
		}
		if len(validationErrors) > 0 {
			log.Printf("CSV parsed with %d validation errors\n", len(validationErrors))
		}
		fmt.Fprintf(w, "Successfully parsed CSV file: %d rows, %d validation errors\n", len(record), len(validationErrors))
	default:
		http.Error(w, "Unsupported file type", http.StatusBadRequest)
	}
}
