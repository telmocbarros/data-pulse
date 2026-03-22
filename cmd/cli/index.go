package main

import (
	"fmt"
	"os"

	playground "github.com/telmocbarros/data-pulse/playground"
)

func main() {
	var userInput string

	for {
		displayMenu()
		fmt.Print("Enter you choice: ")
		fmt.Scanln(&userInput)
		switch userInput {
		case "1":
			fmt.Println("Claude's implementation ...")
			playground.Execute()
		case "2":
			fmt.Println("Fetching with concurrency ...")
			playground.FetchingWithConcurrency()
		case "3":
			fmt.Println("Inserting WITHOUT concurrency ...")
			playground.InsertingWithoutConcurrency()
		case "4":
			fmt.Println("Fetching WITH concurrency ...")
			playground.InsertingWithConcurrency()
		case "q":
			fmt.Println("Goodbye!")
			os.Exit(0)
		default:
			fmt.Println("Invalid option. Try again.")
		}
	}
}

func displayMenu() {
	println("***************************")
	println("* GO Concurrency Tutorial *")
	println("***************************")

	println("1. [playground.go] Claude's Implementation")
	println("2. [recreio.go] Fetching with concurrency")
	println("3. [recreio.go] Inserting without concurrency")
	println("4. [recreio.go] Inserting with concurrency")
	println("q. exit")
}
