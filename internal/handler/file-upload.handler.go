package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	jobRepo "github.com/telmocbarros/data-pulse/internal/repository/job"
	service "github.com/telmocbarros/data-pulse/internal/service/dataset"
	"github.com/telmocbarros/data-pulse/internal/service/jobmanager"
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
	defer file.Close()

	var fileType string
	switch handler.Header.Get("Content-Type") {
	case "application/json":
		fileType = "json"
	case "text/csv":
		fileType = "csv"
	default:
		http.Error(w, "Unsupported file type", http.StatusBadRequest)
		return
	}

	// Spool upload to a temp file so the pipeline can stream it
	// after the HTTP request completes.
	tmpFile, err := os.CreateTemp("", "datapulse-upload-*")
	if err != nil {
		fmt.Println("Error creating temp file:", err)
		http.Error(w, "Error processing upload", http.StatusInternalServerError)
		return
	}

	if _, err := io.Copy(tmpFile, file); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		fmt.Println("Error writing temp file:", err)
		http.Error(w, "Error processing upload", http.StatusInternalServerError)
		return
	}
	tmpFile.Close()

	jobID, err := jobRepo.CreateJob(handler.Filename, fileType)
	if err != nil {
		os.Remove(tmpFile.Name())
		fmt.Println("Error creating job:", err)
		http.Error(w, "Error creating job", http.StatusInternalServerError)
		return
	}

	tmpPath := tmpFile.Name()
	fileName := handler.Filename
	fileSize := handler.Size

	jobmanager.Default.Submit(jobID, func(ctx context.Context, progressFn func(int)) error {
		defer os.Remove(tmpPath)

		f, err := os.Open(tmpPath)
		if err != nil {
			return fmt.Errorf("error opening temp file: %w", err)
		}
		defer f.Close()

		switch fileType {
		case "json":
			return service.ProcessJsonFile(ctx, f, fileName, fileSize, progressFn)
		case "csv":
			return service.ProcessCsvFile(ctx, f, fileName, fileSize, progressFn)
		}
		return nil
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"job_id": "%s"}`, jobID)
}
