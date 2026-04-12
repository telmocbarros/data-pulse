package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	storage "github.com/telmocbarros/data-pulse/config"
	service "github.com/telmocbarros/data-pulse/internal/service/dataset"
)

func main() {
	storage.SetupDatabase()
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
				_, err := service.ProcessCsvFile(context.Background(), bytes.NewReader(data), "sample_data.csv", int64(len(data)), noopProgress)
				if err != nil {
					log.Printf("CSV processing error: %v\n", err)
				} else {
					fmt.Println("Successfully parsed CSV file")
				}
			}

		case "2":
			fmt.Println("Process Json file ...")
			data := loadFile("json")
			if data != nil {
				_, err := service.ProcessJsonFile(context.Background(), bytes.NewReader(data), "sample_data.json", int64(len(data)), noopProgress)
				if err != nil {
					log.Printf("JSON processing error: %v\n", err)
				} else {
					fmt.Println("Successfully parsed JSON file")
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
