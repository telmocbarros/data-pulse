package main

import (
	"fmt"
	"strconv"
	"time"
)

func main() {
	valueType, err := computeVariableType("2024-04-30 12:18:00")
	if err != nil {
		fmt.Print("Error: ", err)
	} else {
		fmt.Print("Type: ", valueType)
	}
}

const (
	IS_NUMERICAL = "IS_NUMERICAL"
	IS_BOOLEAN   = "IS_BOOLEAN"
	IS_DATE      = "IS_DATE"
	IS_TEXT      = "IS_TEXT"
)

func computeVariableType(value string) (valueType string, err error) {
	// is empty
	if len(value) == 0 {
		return IS_TEXT, nil
	}

	// is boolean
	_, err = strconv.ParseBool(value)
	if err == nil {
		return IS_BOOLEAN, nil
	}
	fmt.Println("Error (boolean): ", err)
	// is numerical
	_, err = strconv.ParseFloat(value, 64)
	if err == nil {
		return IS_NUMERICAL, nil
	}
	fmt.Println("Error (float): ", err)

	_, err = strconv.ParseInt(value, 10, 64)
	if err == nil {
		return IS_NUMERICAL, nil
	}
	fmt.Println("Error (int): ", err)

	//is_date
	_, err = time.Parse("2006-01-02 15:04:05", value)
	if err == nil {
		return IS_DATE, nil
	}
	fmt.Println("Error (date): ", err)
	// is date
	_, err = time.Parse("2006-01-02", value)
	if err == nil {
		return IS_DATE, nil
	}
	fmt.Println("Error (date): ", err)

	return IS_TEXT, nil
}
