package playground

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers pgx as a database/sql driver
	"github.com/joho/godotenv"
)

type CreateUserInput struct {
	FirstName   string `db:"first_name"`
	LastName    string `db:"last_name"`
	Email       string `db:"email"`
	DateOfBirth string `db:"date_of_birth"`
	Username    string `db:"username"`
	Password    string `db:"password"`
	Country     string `db:"country"`
	Phone       string `db:"phone"`
}

type UserDB struct {
	UID         string    `db:"uid"`
	FirstName   string    `db:"first_name"`
	LastName    string    `db:"last_name"`
	Email       string    `db:"email"`
	CreatedAt   time.Time `db:"created_at"`
	UpdatedAt   time.Time `db:"updated_at"`
	DateOfBirth time.Time `db:"date_of_birth"`
	Username    string    `db:"username"`
	Password    string    `db:"password"`
	Country     string    `db:"country"`
	Phone       string    `db:"phone"`
}

type UserDTO struct {
	FullName    string `json:"full_name"`
	Email       string `json:"email"`
	Username    string `json:"username"`
	Country     string `json:"country"`
	Phone       string `json:"phone"`
	DateOfBirth string `json:"date_of_birth"`
}

func configDatabase() *sql.DB {
	err := godotenv.Load()
	if err != nil {
		fmt.Print("Error attempting to load environment variables. ", err)
		return nil
	}

	connectionString := os.Getenv("DATABASE_URL")
	if connectionString == "" {
		fmt.Print("Database connection string is not defined.")
		return nil
	}

	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		fmt.Print("Error attempting to connect to the database. ", err)
		return nil
	}

	return db
}

func FetchingWithConcurrency() {
	start := time.Now()
	fmt.Println("Start time: ", start)

	db := configDatabase()
	defer db.Close()

	usersCh := make(chan UserDTO, 100)
	dbUsersCh := make(chan UserDB, 100)

	// fetch users from the database
	go fetchUsers(db, dbUsersCh)

	// transform database users to dto users
	go transformToDtoUser(usersCh, dbUsersCh)

	var cnt int
	for range usersCh {
		cnt++
	}
	fmt.Println("Elapsed time: ", time.Since(start))
}

func InsertingWithoutConcurrency() {
	start := time.Now()
	fmt.Println("Start time: ", start)

	db := configDatabase()
	defer db.Close()

	users := GenerateUsersPayload()
	newUsers := make([]CreateUserInput, 0, len(users))
	for _, userDto := range users {
		user := transformToDbUser(userDto)
		newUsers = append(newUsers, user)
	}

	err := createUsersDB(db, newUsers)
	if err != nil {
		fmt.Println("Error creating new user entities in the database. ", err)
	}

	fmt.Println("Elapsed time: ", time.Since(start))
}

func InsertingWithConcurrency() {
	start := time.Now()

	db := configDatabase()
	defer db.Close()

	usersCh := make(chan UserDTO, 250)
	dbUsersCh := make(chan CreateUserInput, 250)

	go GenerateUserPayloadWithConcurrency(usersCh)

	go func() {
		for userDto := range usersCh {
			user := transformToDbUser(userDto)
			dbUsersCh <- user
		}
		close(dbUsersCh)
	}()

	err := createUsersDBWithConcurrency(db, dbUsersCh)
	if err != nil {
		fmt.Println("Error creating new user entities in the database. ", err)
	}

	fmt.Println("Elapsed time: ", time.Since(start))
}

func fetchUsers(db *sql.DB, dbUsersCh chan<- UserDB) {
	rows, err := db.Query("SELECT uid, first_name, last_name, email, created_at, updated_at, date_of_birth, username, password, country, phone FROM users LIMIT 20")
	if err != nil {
		fmt.Print("Something went wrong when fetching users. ", err)
		return
	}

	defer rows.Close()

	var user UserDB
	for rows.Next() {
		err = rows.Scan(&user.UID, &user.FirstName, &user.LastName, &user.Email, &user.CreatedAt, &user.UpdatedAt, &user.DateOfBirth, &user.Username, &user.Password, &user.Country, &user.Phone)
		if err != nil {
			fmt.Println("Skipped user")
			continue
		}
		// fmt.Println("[fetchUsers]: ", user)
		dbUsersCh <- user
	}

	close(dbUsersCh)
}

func createUsersDB(db *sql.DB, users []CreateUserInput) error {
	query := `INSERT INTO users (first_name, last_name, email, date_of_birth, username, password, country, phone) VALUES `

	batchLimit := 1000
	numBatches := len(users) / batchLimit

	for j := range numBatches {
		values := []any{}
		placeholders := []string{}

		for i, u := range users[j*batchLimit : j*batchLimit+batchLimit] {
			base := i * 8
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8))
			values = append(values, u.FirstName, u.LastName, u.Email, u.DateOfBirth, u.Username, u.Password, u.Country, u.Phone)
		}

		batchQuery := query + strings.Join(placeholders, ", ")

		_, err := db.Exec(batchQuery, values...)
		if err != nil {
			return err
		}
		fmt.Printf("Batch %d/%d inserted\n", j+1, numBatches)
	}
	return nil
}

func createUsersDBWithConcurrency(db *sql.DB, usersCh <-chan CreateUserInput) error {
	query := `INSERT INTO users (first_name, last_name, email, date_of_birth, username, password, country, phone) VALUES `
	batchCounter := 0
	numBatches := 0
	batchLimit := 1000
	batch := make([]CreateUserInput, 0, batchLimit)

	for value := range usersCh {
		batch = append(batch, value)
		batchCounter++
		if batchCounter == batchLimit {
			values := []any{}
			placeholders := []string{}
			for i, u := range batch {
				base := i * 8
				placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
					base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8))
				values = append(values, u.FirstName, u.LastName, u.Email, u.DateOfBirth, u.Username, u.Password, u.Country, u.Phone)
			}

			batchQuery := query + strings.Join(placeholders, ", ")

			_, err := db.Exec(batchQuery, values...)
			if err != nil {
				return err
			}

			batchCounter = 0
			batch = batch[:0]
			numBatches++
			fmt.Printf("Batch %d inserted\n", numBatches)
		}
	}

	if len(batch) > 0 {
		values := []any{}
		placeholders := []string{}
		for i, u := range batch {
			base := i * 8
			placeholders = append(placeholders, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				base+1, base+2, base+3, base+4, base+5, base+6, base+7, base+8))
			values = append(values, u.FirstName, u.LastName, u.Email, u.DateOfBirth, u.Username, u.Password, u.Country, u.Phone)
		}

		batchQuery := query + strings.Join(placeholders, ", ")

		_, err := db.Exec(batchQuery, values...)
		if err != nil {
			return err
		}
		numBatches++
		fmt.Printf("Batch %d inserted (remainder: %d users)\n", numBatches, len(batch))
	}

	return nil
}

func transformToDtoUser(usersCh chan<- UserDTO, dbUsersCh <-chan UserDB) {
	for value := range dbUsersCh {
		newUser := UserDTO{
			DateOfBirth: value.DateOfBirth.GoString(),
			FullName:    value.FirstName + " " + value.LastName,
			Country:     value.Country,
			Email:       value.Email,
			Phone:       value.Phone,
			Username:    value.Username,
		}
		usersCh <- newUser
	}
	close(usersCh)
}

func transformToDbUser(userDto UserDTO) CreateUserInput {
	names := strings.SplitN(userDto.FullName, " ", 2)
	return CreateUserInput{
		FirstName:   names[0],
		LastName:    names[1],
		Email:       userDto.Email,
		DateOfBirth: userDto.DateOfBirth,
		Username:    userDto.Username,
		Country:     userDto.Country,
		Phone:       userDto.Phone,
	}
}
