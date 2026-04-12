package handler

import (
	"fmt"
	"net/http"

	service "github.com/telmocbarros/data-pulse/internal/service/dataset"
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
		if err := service.ProcessJsonFileAsync(file, handler.Filename, handler.Size); err != nil {
			fmt.Println("Error parsing JSON file: ", err)
			http.Error(w, "Error parsing JSON file", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status": "accepted"}`)

	case "text/csv":
		if err := service.ProcessCsvFileAsync(file, handler.Filename, handler.Size); err != nil {
			fmt.Println("Error parsing CSV file: ", err)
			http.Error(w, "Error parsing CSV file", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, `{"status": "accepted"}`)
	default:
		http.Error(w, "Unsupported file type", http.StatusBadRequest)
	}
}
