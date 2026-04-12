package handler

import (
	"encoding/json"
	"net/http"

	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
)

// ListDatasetsHandler returns metadata for all datasets.
// GET /api/datasets/
func ListDatasetsHandler(w http.ResponseWriter, r *http.Request) {
	datasets, err := repository.ListDatasets()
	if err != nil {
		http.Error(w, "Error fetching datasets", http.StatusInternalServerError)
		return
	}

	if datasets == nil {
		datasets = []map[string]any{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(datasets)
}
