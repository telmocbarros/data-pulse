package playground

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers pgx as a database/sql driver
	"github.com/joho/godotenv"
)

type User struct {
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

type FormattedUser struct {
	UID      string `json:"uid"`
	FullName string `json:"full_name"`
	Email    string `json:"email"`
	Username string `json:"username"`
	Country  string `json:"country"`
	Phone    string `json:"phone"`
	Age      int    `json:"age"`
}

// Wire it up
func Execute() {
	err := godotenv.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to fetch configuration data: %v\n", err)
		os.Exit(1)
	}

	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	raw := make(chan User)
	formatted := make(chan FormattedUser)

	fmt.Println("Fetching users from DB")
	go getUsers(db, raw) // runs on its own goroutine
	fmt.Println("Transforming users from DB")
	go transform(raw, formatted) // runs on its own goroutine

	for user := range formatted { // consume results
		fmt.Println(user)
	}
}

func toOtherFormat(u User) FormattedUser {
	return FormattedUser{
		UID:      u.UID,
		FullName: u.FirstName + " " + u.LastName,
		Email:    u.Email,
		Username: u.Username,
		Country:  u.Country,
		Phone:    u.Phone,
		Age:      int(time.Since(u.DateOfBirth).Hours() / 24 / 365),
	}
}

// Stage 1: Fetch users from DB and send them to a channel
func getUsers(db *sql.DB, out chan<- User) {
	rows, err := db.Query("SELECT uid, first_name, last_name, email, created_at, updated_at, date_of_birth, username, password, country, phone FROM users LIMIT 20")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
		close(out)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var u User
		err := rows.Scan(&u.UID, &u.FirstName, &u.LastName, &u.Email, &u.CreatedAt, &u.UpdatedAt, &u.DateOfBirth, &u.Username, &u.Password, &u.Country, &u.Phone)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Scan failed: %v\n", err)
			continue
		}
		fmt.Printf("[getUsers]    %s - sending user %s\n", time.Now().Format("15:04:05.000"), u.Username)
		out <- u
	}

	close(out)
}

// Stage 2: Read from one channel, transform, send to another
// in <-chan User: receive only channel declaration
// out chan <- FormattedUser: this is a send-only channel
// declaring without the arrow, it declares a bidirectional channel
func transform(in <-chan User, out chan<- FormattedUser) {
	for user := range in { // like "for await...of"
		fmt.Printf("[transform]   %s - formatting user %s\n", time.Now().Format("15:04:05.000"), user.Username)
		out <- toOtherFormat(user)
	}
	close(out)
}
