package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/health", health)
	http.HandleFunc("/dataset", uploadDatasetInChunks)
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
