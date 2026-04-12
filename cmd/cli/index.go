package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	storage "github.com/telmocbarros/data-pulse/config"
	service "github.com/telmocbarros/data-pulse/internal/service/dataset"
)

/*
- You need to wrap the []byte to satisfy multipart.File. But actually, the service functions only use io.Reader capabilities (csv.NewReader(f), json.NewDecoder(f)).
- The cleaner fix would be to change the service to accept io.ReadSeekCloser instead of multipart.File — then both the API and CLI can use it.
- bytesFile wraps bytes.Reader (which already satisfies io.Reader, io.ReaderAt, io.Seeker)
- and adds a no-op Close() — that's all multipart.File
*/
type bytesFile struct {
	*bytes.Reader
}

func (b bytesFile) Close() error { return nil }

func main() {
	storage.SetupDatabase()
	var userInput string

	for {
		displayMenu()
		fmt.Print("Enter you choice: ")
		fmt.Scanln(&userInput)
		switch userInput {
		case "1":
			fmt.Println("Process CSV file ...")
			data := loadFile("csv")
			if data != nil {
				validationErrors := service.ProcessCsvFileSync(bytesFile{bytes.NewReader(data)}, "sample_data.csv", int64(len(data)))
				if len(validationErrors) > 0 {
					log.Printf("CSV parsed with %d validation errors\n", len(validationErrors))
				}
				fmt.Printf("Successfully parsed CSV file, %d validation errors\n", len(validationErrors))

			}

		case "2":
			fmt.Println("Process Json file ...")
			data := loadFile("json")
			if data != nil {
				validationErrors := service.ProcessJsonFileSync(bytesFile{bytes.NewReader(data)}, "sample_data.json", int64(len(data)))
				if len(validationErrors) > 0 {
					log.Printf("JSON parsed with %d validation errors\n", len(validationErrors))
				}
				fmt.Printf("Successfully parsed JSON file, %d validation errors\n", len(validationErrors))
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
