package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/google/uuid"
	"github.com/xuri/excelize/v2"
)

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("database connection string is not set")
	}

	f, err := excelize.OpenFile("filename.xlsx")
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
		if row[1] != "Name" && row[2] != "Telephone 1" && row[4] != "Amount" && row[2] != "" {
			persons = append(persons, &Person{
				FullNames: row[1],
				Phone:     row[2],
				Amount:    row[4],
			})
		}
	}

	for _, p := range persons {

		fname := strings.Split(p.FullNames, " ")
		if len(fname) > 1 {
			p.FirstName = fname[0]
			if fname[1] == "" && len(fname) > 2 {
				p.SecondName = fname[2]
			} else {
				p.SecondName = strings.Trim(fmt.Sprint(fname[1:]), "[]")

			}
		} else {
			fname = strings.Split(p.FullNames, ".")
			if len(fname) > 1 {
				p.FirstName = fname[0]
				if fname[1] == "" && len(fname) > 2 {
					p.SecondName = fname[2]
				} else {
					p.SecondName = strings.Trim(fmt.Sprint(fname[1:]), "[]")
				}
			} else {
				p.FirstName = p.FullNames
				p.SecondName = p.FullNames
			}
		}

		p.Amount = strings.TrimSuffix(p.Amount, "RWF")
		p.RecordedBy = "username" //eg:078xxx
		p.NameSpace = "namespace" //eg:kigali.gasabo.gatsata
		p.ForRent = true
		p.Sector = "sector-name"   //eg:Gatsata
		p.Cell = "cell-name"       //eg:Karuruma
		p.Village = "village-name" //eg:Rugoro

		if !strings.HasPrefix(p.Phone, "0") {
			p.Phone = "0" + p.Phone
		}
		p.Phone = strings.ReplaceAll(p.Phone, ",", "")
	}
	log.Printf("connecting to database")

	phoneMap := make(map[string]bool)
	uniquePersons := make([]*Person, 0)
	duplicatedPersons := make([]*Person, 0)

	for _, p := range persons {
		if !phoneMap[p.Phone] {
			phoneMap[p.Phone] = true
			p.Id = uuid.New().String()
			uniquePersons = append(uniquePersons, p)
		} else {
			duplicatedPersons = append(duplicatedPersons, p)
		}
	}

	db := New(ctx, dsn)
	if err := db.Open(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Printf("connected to database")

	for index, p := range uniquePersons {
		log.Printf("%d. FirstName: %s, SecondName: %s, Phone: %s, Amount: %s\n", index+1, p.FirstName, p.SecondName, p.Phone, p.Amount)
	}
	log.Printf("inserting above people into database")

	// insert persons into database
	res, err := db.insertPersons(ctx, uniquePersons)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("inserted %d persons", len(res))

	log.Println("Add ids to duplicated persons")
	for _, p := range duplicatedPersons {
		for _, r := range res {
			if p.Phone == r.Phone {
				p.Id = r.Id
			}
		}
	}

	log.Println("inserting above people into properties")

	allPersons := removeDuplicates(append(res, duplicatedPersons...))
	// insert persons into prop	erties
	out, err := db.insertPortperties(ctx, allPersons)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("inserted %d properties", len(out))

	os.Exit(0)
	db.Close()
}

func removeDuplicates(s []*Person) []*Person {

	encountered := map[Person]bool{}
	result := make([]*Person, 0)
	for _, v := range s {
		if !encountered[*v] {
			encountered[*v] = true
			result = append(result, v)
		}
	}
	return result
}
