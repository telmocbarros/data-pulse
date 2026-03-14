package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/health", health)
	http.HandleFunc("/dataset", fileUploadHandler)
	fmt.Println("Listening on PORT 8080 ...")
	http.ListenAndServe(":8080", nil)
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("healthy"))
}

func uploadDataset(w http.ResponseWriter, r *http.Request) {
	// 1. Parse input, type multipart/form-data

	// << is a bitwise operator. By putting a number to its right,
	// you're shifting the number on its left in its binary representation
	// by that amount of zeros
	// Usual patterns:
	// << 10 = ×1,024 (KiB)
	// << 20 = ×1,048,576 (MiB)
	// << 30 = ×1,073,741,824 (GiB)
	r.ParseMultipartForm(10 << 20)

	// 2. Retrieve file from posted form-data
	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println("Error retrieving file: ", err)
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	defer file.Close()

	mimeType := handler.Header.Get("Content-Type")
	if mimeType == "" {
		fmt.Println("Invalid file type: (empty)")
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	} else if mimeType != "text/csv" && mimeType != "application/json" {
		fmt.Println("Invalid file type: ", mimeType)
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	}

	// 3. Write temporary file on our server
	// %#v	a Go-syntax representation of the value
	// (floating-point infinities and NaNs print as ±Inf and NaN)
	fmt.Printf("File name: %+v\n", handler.Filename)
	fmt.Printf("File size: %+v B\n", handler.Size)
	fmt.Printf("MIME Header: %+v\n", handler.Header)
	fmt.Printf("Content Type: %s\n", handler.Header.Get("Content-Type"))

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
		http.Error(w, "Error reading file", http.StatusInternalServerError)
		return
	}

	fileToCreate, err := os.Create("temp.txt")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
		http.Error(w, "Error creating file", http.StatusInternalServerError)
		return
	}

	defer fileToCreate.Close()

	// 4. Return whether or not this has been succesfull
	_, err = fileToCreate.Write(fileBytes)
	if err != nil {
		log.Fatalf("Error writing to file: %v", err)
		http.Error(w, "Error writing to file", http.StatusInternalServerError)

	}

	fmt.Fprintf(w, "Successfully Uploaded File\n")
}

func uploadDatasetInChunks(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		fmt.Println("Error parsing file: ", err)
		http.Error(w, "Error parsing file", http.StatusBadRequest)
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println("Error retrieving file: ", err)
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
	}
	defer file.Close()

	mimeType := handler.Header.Get("Content-Type")
	if mimeType == "" {
		fmt.Println("Invalid file type: (empty)")
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	} else if mimeType != "text/csv" && mimeType != "application/json" {
		fmt.Println("Invalid file type: ", mimeType)
		http.Error(w, "Invalid file type", http.StatusBadRequest)
		return
	}
	fileToCreate, err := os.Create("temp.txt")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
		http.Error(w, "Error creating file", http.StatusInternalServerError)
		return
	}
	defer fileToCreate.Close()

	written, err := io.Copy(fileToCreate, file)
	if err != nil {
		log.Fatalf("Error writing to file: %v", err)
		http.Error(w, "Error writing to file", http.StatusInternalServerError)
		return
	}

	fmt.Printf("Succesfully uploaded file: %v B\n", written)
	fmt.Fprintf(w, "Successfully Uploaded File\n")
}

func parseCsvFile(f multipart.File) (results [][]string, err error) {
	csvReader := csv.NewReader(f)

	for {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatalf("Error parsing the csv file: %v", err)
				return nil, err
			}
		}
		results = append(results, record)
		fmt.Println(record)
	}
	return results, nil
}

func parseJsonFile(f multipart.File) (record any, err error) {
	jsonBytes, err := io.ReadAll(f)
	if err != nil {
		log.Fatal("Error converting the file into byte format", err)
		return nil, err
	}

	f.Read(jsonBytes)
	fmt.Println(string(jsonBytes))

	var result map[string]any
	err = json.Unmarshal(jsonBytes, &result)
	if err != nil {
		log.Fatal("Error parsing json file: ", err)
		return nil, err
	}

	fmt.Println(result)
	return result, nil
}

func fileUploadHandler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(10 << 20)
	if err != nil {
		log.Fatal("Error retrieving multipart form")
		http.Error(w, "Error retrieving multipart form", http.StatusBadRequest)
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		log.Fatal("Error retrieving file: ", err)
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
	}
	fmt.Println("content type: ", handler.Header.Get("Content-Type"))

	switch handler.Header.Get("Content-Type") {
	case "application/json":
		record, err := parseJsonFile(file)
		if err != nil {
			log.Fatal("Error parsing JSON file: ", err)
			http.Error(w, "Error parsing JSON file", http.StatusInternalServerError)
			return
		}
		jsonBytes, err := json.Marshal(record)
		if err != nil {
			log.Fatal("Error parsing csv file: ", err)
			http.Error(w, "Error encoding JSON response", http.StatusInternalServerError)
			return
		}
		w.Write(jsonBytes)
	case "text/csv":
		record, err := parseCsvFile(file)
		if err != nil {
			http.Error(w, "Error parsing csv file", http.StatusInternalServerError)
		}
		w.Write([]byte(fmt.Sprintf("%v", record)))
	}
}
