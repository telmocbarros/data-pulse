package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

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
	id, ok := parseUUIDPath(w, r)
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
	id, ok := parseUUIDPath(w, r)
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

// validationErrorOut is the JSON shape returned for one validation error.
type validationErrorOut struct {
	Row      int32  `json:"row"`
	Column   int32  `json:"column"`
	Kind     string `json:"kind"`
	Expected string `json:"expected,omitempty"`
	Received string `json:"received,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// validationErrorsResponse pairs the page with its paging cursor.
type validationErrorsResponse struct {
	Limit  int                  `json:"limit"`
	Offset int                  `json:"offset"`
	Errors []validationErrorOut `json:"errors"`
}

// ListValidationErrorsHandler returns a page of validation errors recorded
// during the dataset's ingestion. Query params: limit (default 50, max 500),
// offset (default 0).
// GET /api/datasets/{id}/validation-errors
func ListValidationErrorsHandler(w http.ResponseWriter, r *http.Request) {
	id, ok := parseUUIDPath(w, r)
	if !ok {
		return
	}

	limit := 50
	offset := 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			offset = n
		}
	}

	errs, err := repository.ListValidationErrors(id, limit, offset)
	if err != nil {
		log.Println("ListValidationErrors error:", err)
		http.Error(w, "Error fetching validation errors", http.StatusInternalServerError)
		return
	}

	out := make([]validationErrorOut, len(errs))
	for i, e := range errs {
		out[i] = validationErrorOut{
			Row:      e.Row,
			Column:   e.Column,
			Kind:     e.Kind,
			Expected: e.Expected,
			Received: e.Received,
			Detail:   e.Detail,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(validationErrorsResponse{
		Limit:  limit,
		Offset: offset,
		Errors: out,
	})
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
	id, ok := parseUUIDPath(w, r)
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

// parseUUIDPath extracts and validates the {id} path parameter as a UUID.
// On failure it writes a 400 response and returns ok = false.
func parseUUIDPath(w http.ResponseWriter, r *http.Request) (string, bool) {
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
