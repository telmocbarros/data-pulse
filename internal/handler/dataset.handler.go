package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
	service "github.com/telmocbarros/data-pulse/internal/service/dataset"
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

// GetDatasetHandler returns metadata for a specific dataset.
// GET /api/datasets/:id/
func GetDatasetHandler(w http.ResponseWriter, r *http.Request) {
	datasetId := r.PathValue("id")
	if datasetId == "" {
		http.Error(w, "Invalid parameter", http.StatusBadRequest)
		return
	}
	if _, err := uuid.Parse(datasetId); err != nil {
		log.Println("Error parsing the id:", err)
		http.Error(w, "Invalid parameter", http.StatusBadRequest)
		return
	}

	query := r.URL.Query()
	graphType := query.Get("graphtype")
	dataset, err := service.GetSingleDataset(datasetId, graphType, query)
	if err != nil {
		log.Println("GetSingleDataset error:", err)
		http.Error(w, "Server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dataset)
}
