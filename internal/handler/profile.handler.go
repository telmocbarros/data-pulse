package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
	profilerRepo "github.com/telmocbarros/data-pulse/internal/repository/profiler"
	"github.com/telmocbarros/data-pulse/internal/service/profiler"
)

// GetProfileHandler returns the stored profiling results for a dataset.
// GET /api/datasets/{id}/profile
func GetProfileHandler(w http.ResponseWriter, r *http.Request) {
	id := extractDatasetId(r.URL.Path)
	if id == "" {
		http.Error(w, "Invalid dataset ID", http.StatusBadRequest)
		return
	}

	profile, err := profilerRepo.GetProfile(id)
	if err != nil {
		http.Error(w, "Profile not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(profile)
}

// CreateProfileHandler re-profiles an existing dataset by reading rows
// from the database and running the profiler.
// POST /api/datasets/{id}/profile
func CreateProfileHandler(w http.ResponseWriter, r *http.Request) {
	id := extractDatasetId(r.URL.Path)
	if id == "" {
		http.Error(w, "Invalid dataset ID", http.StatusBadRequest)
		return
	}

	tableName, columnTypes, err := repository.GetDatasetById(id)
	if err != nil {
		http.Error(w, "Dataset not found", http.StatusNotFound)
		return
	}

	rows, err := repository.GetDatasetRows(tableName)
	if err != nil {
		fmt.Println("Error reading dataset rows:", err)
		http.Error(w, "Error reading dataset", http.StatusInternalServerError)
		return
	}

	// Feed rows into a channel for the profiler
	rowCh := make(chan map[string]any, 100)
	go func() {
		defer close(rowCh)
		for _, row := range rows {
			rowCh <- row
		}
	}()

	result := profiler.ProfileDataset(rowCh, columnTypes)

	if err := profilerRepo.StoreProfile(id, result); err != nil {
		fmt.Println("Error storing profile:", err)
		http.Error(w, "Error storing profile", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(result)
}

// extractDatasetId extracts the dataset ID from paths like /api/datasets/{id}/profile
func extractDatasetId(path string) string {
	path = strings.TrimPrefix(path, "/api/datasets/")
	id := strings.TrimSuffix(path, "/profile")
	if id == "" || strings.Contains(id, "/") {
		return ""
	}
	return id
}
