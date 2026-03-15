package service

import "fmt"

func UploadDataset(datasetType string, dataset any) {
	switch datasetType {
	case "csv":
		if data, ok := dataset.([][]string); ok {
			fmt.Println("Processing csv dataset ...")
			uploadCsvDataset(data)
		} else {
			fmt.Println("Mismatch between dataset type and dataset format")
		}
	case "json":
		if data, ok := dataset.(map[string]any); ok {
			fmt.Println("Processing json dataset ...")
			fmt.Println("Processing csv dataset ...")
			uploadJsonDataset(data)
		} else {
			fmt.Println("Mismatch between dataset type and dataset format")
		}
	default:
		fmt.Println("Invalid dataset type: ", datasetType)
	}
}

func uploadCsvDataset(dataset [][]string) {
	// columnNames := dataset[0][0]
	// create table
}

func uploadJsonDataset(dataset map[string]any) {

}
