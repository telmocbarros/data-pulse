package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/telmocbarros/data-pulse/config"
	repository "github.com/telmocbarros/data-pulse/internal/repository/dataset_upload"
	service "github.com/telmocbarros/data-pulse/internal/service/dataset"
	profilerService "github.com/telmocbarros/data-pulse/internal/service/profiler"
)

func main() {
	if err := config.SetupDatabase(); err != nil {
		log.Fatalf("Error setting up the database: %v", err)
	}
	defer config.Storage.Close()

	if err := config.SetupFileStorage(); err != nil {
		log.Fatalf("Error setting up file storage: %v", err)
	}
	var userInput string

	noopProgress := func(int) {}

	for {
		displayMenu()
		fmt.Print("Enter you choice: ")
		fmt.Scanln(&userInput)
		switch userInput {
		case "1":
			fmt.Println("Process CSV file ...")
			data := loadFile("csv")
			if data != nil {
				datasetId, err := service.ProcessCsvFile(context.Background(), bytes.NewReader(data), "sample_data.csv", int64(len(data)), noopProgress)
				if err != nil {
					log.Printf("CSV processing error: %v\n", err)
				} else {
					fmt.Println("Successfully parsed CSV file")
					runProfiler(datasetId)
				}
			}

		case "2":
			fmt.Println("Process Json file ...")
			data := loadFile("json")
			if data != nil {
				datasetId, err := service.ProcessJsonFile(context.Background(), bytes.NewReader(data), "sample_data.json", int64(len(data)), noopProgress)
				if err != nil {
					log.Printf("JSON processing error: %v\n", err)
				} else {
					fmt.Println("Successfully parsed JSON file")
					runProfiler(datasetId)
				}
			}
		case "q":
			fmt.Println("Goodbye!")
			os.Exit(0)
		default:
			fmt.Println("Invalid option. Try again.")
		}
	}
}

func displayMenu() {
	println("**************")
	println("* Data Pulse *")
	println("**************")

	println("1. Process CSV File")
	println("2. Process Json File")
	println("q. exit")
}

func runProfiler(datasetId string) {
	tableName, columnTypes, err := repository.GetDatasetById(datasetId)
	if err != nil {
		log.Printf("Error fetching dataset for profiling: %v\n", err)
		return
	}
	if err := profilerService.ProfileAndStore(datasetId, tableName, columnTypes); err != nil {
		log.Printf("Error profiling dataset: %v\n", err)
		return
	}
	fmt.Println("Successfully profiled dataset")
}

func loadFile(fileExtension string) []byte {
	var path string
	switch fileExtension {
	case "csv":
		path = "./sample_data.csv"
	case "json":
		path = "./sample_data.json"
	default:
		fmt.Println("The provided file extension is invalid. Please choose csv or json.")
		return nil
	}

	file, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("Error reading %v file: %v\n", fileExtension, err)
		return nil
	}
	return file
}
