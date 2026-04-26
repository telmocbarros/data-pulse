package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/google/uuid"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
	service "github.com/telmocbarros/data-pulse/internal/service/dataset"
)

// ListDatasetsHandler returns metadata for all alive datasets.
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

// GetDatasetHandler returns the metadata + schema for a single dataset.
// GET /api/datasets/{id}
func GetDatasetHandler(w http.ResponseWriter, r *http.Request) {
	id, ok := parseDatasetID(w, r)
	if !ok {
		return
	}

	metadata, err := service.GetDatasetMetadata(id)
	if err != nil {
		log.Println("GetDatasetMetadata error:", err)
		http.Error(w, "Dataset not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metadata)
}

// DeleteDatasetHandler soft-deletes a dataset (sets deleted_at = NOW()).
// DELETE /api/datasets/{id}
func DeleteDatasetHandler(w http.ResponseWriter, r *http.Request) {
	id, ok := parseDatasetID(w, r)
	if !ok {
		return
	}

	if err := repository.SoftDeleteDataset(id); err != nil {
		log.Println("SoftDeleteDataset error:", err)
		http.Error(w, "Dataset not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// visualizeRequest is the body shape for POST /api/datasets/{id}/visualize.
// Params is decoded into the chart-type-specific struct in the dispatcher.
type visualizeRequest struct {
	Type   service.VisualizationType `json:"type"`
	Params json.RawMessage           `json:"params"`
}

// VisualizeDatasetHandler dispatches a visualization request to the matching
// service function based on req.Type.
// POST /api/datasets/{id}/visualize
func VisualizeDatasetHandler(w http.ResponseWriter, r *http.Request) {
	id, ok := parseDatasetID(w, r)
	if !ok {
		return
	}

	var req visualizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if !req.Type.IsValid() {
		http.Error(w, "Unknown visualization type", http.StatusBadRequest)
		return
	}

	result, err := dispatchVisualize(id, req)
	if err != nil {
		log.Println("Visualize error:", err)
		// Validation errors (bad column, missing required field) come from the
		// service layer's resolve() methods. We can't easily distinguish those
		// from infra errors here without typed errors, so return 400 — most
		// service errors today are caller-side. Worth tightening later.
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// dispatchVisualize routes the request to the matching service function and
// returns the typed result (which json.Encoder serializes per-type).
func dispatchVisualize(id string, req visualizeRequest) (any, error) {
	switch req.Type {
	case service.VizHistogram:
		var p service.HistogramParams
		if err := decodeParams(req.Params, &p); err != nil {
			return nil, err
		}
		return service.GetHistogram(id, p)
	case service.VizScatter:
		var p service.ScatterParams
		if err := decodeParams(req.Params, &p); err != nil {
			return nil, err
		}
		return service.GetScatter(id, p)
	case service.VizTimeseries:
		var p service.TimeseriesParams
		if err := decodeParams(req.Params, &p); err != nil {
			return nil, err
		}
		return service.GetTimeseries(id, p)
	case service.VizCorrelationMatrix:
		var p service.CorrelationMatrixParams
		if err := decodeParams(req.Params, &p); err != nil {
			return nil, err
		}
		return service.GetCorrelationMatrix(id, p)
	case service.VizCategoryBreakdown:
		var p service.CategoryBreakdownParams
		if err := decodeParams(req.Params, &p); err != nil {
			return nil, err
		}
		return service.GetCategoryBreakdown(id, p)
	}
	return nil, errors.New("unreachable: validated above")
}

// decodeParams handles the empty-body case (Params == nil) gracefully so
// callers can omit the field for charts whose params are all defaultable.
func decodeParams(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, dst)
}

// parseDatasetID extracts and validates the {id} path parameter. On failure
// it writes the appropriate 4xx response and returns ok = false.
func parseDatasetID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if id == "" {
		http.Error(w, "Invalid parameter", http.StatusBadRequest)
		return "", false
	}
	if _, err := uuid.Parse(id); err != nil {
		http.Error(w, "Invalid parameter", http.StatusBadRequest)
		return "", false
	}
	return id, true
}
