package playground

import (
	"fmt"
	"math/rand"
	"time"
)

var countries = []string{"US", "UK", "PT", "FR", "DE", "ES", "IT", "BR", "JP", "CA"}
var firstNames = []string{"James", "Maria", "John", "Ana", "Pedro", "Sofia", "Liam", "Emma", "Noah", "Olivia"}
var lastNames = []string{"Smith", "Silva", "Johnson", "Garcia", "Brown", "Martinez", "Jones", "Lopez", "Davis", "Wilson"}

func GenerateUsersPayload() []UserDTO {
	users := make([]UserDTO, 1_000_000)

	for i := range users {
		first := firstNames[rand.Intn(len(firstNames))]
		last := lastNames[rand.Intn(len(lastNames))]
		year := 1986 + rand.Intn(21) // 1986 to 2006
		day := 1 + rand.Intn(365)
		date_of_birth := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, day).Format(time.DateOnly)

		country := countries[rand.Intn(len(countries))]

		users[i] = UserDTO{
			FullName:    first + " " + last,
			Email:       fmt.Sprintf("%s.%s%d@example.com", first, last, i),
			Username:    fmt.Sprintf("%s%s%d", first, last, i),
			Country:     country,
			Phone:       fmt.Sprintf("+1%010d", 1000000000+i),
			DateOfBirth: date_of_birth,
		}
	}

	return users
}

func GenerateUserPayloadWithConcurrency(usersCh chan<- UserDTO) {
	users := make([]UserDTO, 1_000_000)
	for i := range users {
		first := firstNames[rand.Intn(len(firstNames))]
		last := lastNames[rand.Intn(len(lastNames))]
		year := 1986 + rand.Intn(21) // 1986 to 2006
		day := 1 + rand.Intn(365)
		date_of_birth := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, day).Format(time.DateOnly)

		country := countries[rand.Intn(len(countries))]

		usersCh <- UserDTO{
			FullName:    first + " " + last,
			Email:       fmt.Sprintf("%s.%s%d@example.com", first, last, i),
			Username:    fmt.Sprintf("%s%s%d", first, last, i),
			Country:     country,
			Phone:       fmt.Sprintf("+1%010d", 1000000000+i),
			DateOfBirth: date_of_birth,
		}
	}
	close(usersCh)
}
