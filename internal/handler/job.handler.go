package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	jobRepo "github.com/telmocbarros/data-pulse/internal/repository/job"
	"github.com/telmocbarros/data-pulse/internal/service/jobmanager"
)

// GetJobHandler returns the status and progress of a job.
// GET /api/jobs/{id}
func GetJobHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	if id == "" || strings.Contains(id, "/") {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	job, err := jobRepo.GetJob(id)
	if err != nil {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

// CancelJobHandler cancels a running job.
// POST /api/jobs/{id}/cancel
func CancelJobHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	id := strings.TrimSuffix(path, "/cancel")

	if !jobmanager.Default.Cancel(id) {
		http.Error(w, "Job not found or not running", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status": "cancelled"}`))
}
