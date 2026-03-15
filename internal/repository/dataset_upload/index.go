package repository

import (
	"github.com/telmocbarros/data-pulse/config"
	"fmt"
	"log"
)

func CreateDatasetTable() {

	result, err := config.Storage.Exec("create table if not exists student (id serial primary key, name varchar(50))")
	if err != nil {
		log.Fatal("unable to execute search query", err)
	}
	fmt.Println("Successfully executed the query: ", result)
}
