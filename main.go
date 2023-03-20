package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/xuri/excelize/v2"
)

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("database connection string is not set")
	}

	f, err := excelize.OpenFile("AKAMAMANA.xlsx")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Get all the rows in the Sheet1.
	rows, err := f.GetRows("Sheet1")
	if err != nil {
		log.Fatal(err)
	}

	persons := make([]*Person, 0)
	for _, row := range rows {
		if row[1] != "Names" && row[2] != "Telephone 1" && row[3] != "Amount Payment" && row[2] != "" {
			persons = append(persons, &Person{
				FullNames: row[1],
				Phone:     row[2],
				Amount:    row[3],
			})
		}
	}

	for _, p := range persons {

		fname := strings.Split(p.FullNames, " ")
		if len(fname) > 1 {
			p.FirstName = fname[0]
			p.SecondName = fname[1]
		} else {
			p.FirstName = p.FullNames
			p.SecondName = p.FullNames
		}

		p.Amount = strings.TrimSuffix(p.Amount, "RWF")
		p.RecordedBy = "gatsata@2023.com"
		p.NameSpace = "kigali.gasabo.gatsata"
		p.ForRent = true
		p.Sector = "Gatsata"
		p.Cell = "Karuruma"
		p.Village = "Akamamana"

		if !strings.HasPrefix(p.Phone, "0") {
			p.Phone = "0" + p.Phone
		}
	}
	log.Printf("connecting to database")

	db := New(ctx, dsn)
	if err := db.Open(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Printf("connected to database")

	for _, p := range persons {
		log.Println(p)
	}

	log.Printf("inserting persons into database")
	// insert persons into database
	res, err := db.insertPersons(ctx, persons)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("inserted %d persons", len(res))

	// //insert persons into properties
	// res, err = db.insertPortperties(ctx, res)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// log.Printf("inserted %d properties", len(res))

	os.Exit(0)
	db.Close()
}
