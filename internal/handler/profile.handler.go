package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset"
	jobRepo "github.com/telmocbarros/data-pulse/internal/repository/job"
	profilerRepo "github.com/telmocbarros/data-pulse/internal/repository/profiler"
	"github.com/telmocbarros/data-pulse/internal/service/jobmanager"
	profilerService "github.com/telmocbarros/data-pulse/internal/service/profiler"
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

// CreateProfileHandler submits a profiling job for an existing dataset.
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

	jobID, err := jobRepo.CreateJob(id, "profile")
	if err != nil {
		fmt.Println("Error creating profiling job:", err)
		http.Error(w, "Error creating profiling job", http.StatusInternalServerError)
		return
	}

	jobmanager.Default.Submit(jobID, func(ctx context.Context, progressFn func(int)) error {
		return profilerService.ProfileAndStore(id, tableName, columnTypes)
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"job_id": "%s"}`, jobID)
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
