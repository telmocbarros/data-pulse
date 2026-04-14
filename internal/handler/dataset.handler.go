package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/google/uuid"
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

// GetDatasetHandler returns metadata for a specific dataset.
// GET /api/datasets/:id/
func GetDatasetHandler(w http.ResponseWriter, r *http.Request) {
	datasetId, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		log.Println("Error parsing the dataset Id: ", err)
		http.Error(w, "Invalid parameter", http.StatusBadRequest)
	}

	log.Printf("Dataset id %v", datasetId)
	w.Write([]byte(datasetId.String()))
}
