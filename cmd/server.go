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
	http.HandleFunc("/dataset", uploadDataset)
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
		fmt.Println(err)
		return
	}
	defer file.Close()

	// 3. Write temporary file on our server
	// %#v	a Go-syntax representation of the value
	// (floating-point infinities and NaNs print as ±Inf and NaN)
	fmt.Printf("File name: %+v\n", handler.Filename)
	fmt.Printf("File size: %+v B\n", handler.Size)
	fmt.Printf("MIME Header: %+v\n", handler.Header)

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	fileToCreate, err := os.Create("temp.txt")
	if err != nil {
		log.Fatalf("Error creating file: %v", err)
	}

	defer fileToCreate.Close()

	// 4. Return whether or not this has been succesfull
	_, err = fileToCreate.Write(fileBytes)
	if err != nil {
		log.Fatalf("Error writing to temp file: %v", err)
	}

	fmt.Fprintf(w, "Successfully Uploaded File\n")
}
