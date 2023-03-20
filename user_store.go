package main

import (
	"context"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// This will hold citizen information
type Person struct {
	Id         string `json:"id,omitempty"`
	FullNames  string `json:"full_names,omitempty"`
	FirstName  string `json:"first_name,omitempty"`
	SecondName string `json:"second_name,omitempty"`
	Phone      string `json:"phone,omitempty"`
	Amount     string `json:"amount,omitempty"`
	Sector     string `json:"sector,omitempty"`
	Cell       string `json:"cell,omitempty"`
	Village    string `json:"village,omitempty"`
	ForRent    bool   `json:"for_rent,omitempty"`
	NameSpace  string `json:"name_space,omitempty"`
	RecordedBy string `json:"recorded_by,omitempty"`
}

// Insert persons into database
func (db *DB) insertPersons(ctx context.Context, persons []*Person) ([]*Person, error) {

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, item := range persons {
		err := tx.QueryRowContext(
			ctx,
			inserOwnerQuery,
			uuid.New().String(),
			item.FirstName,
			item.SecondName,
			item.Phone,
		).Scan(&item.Id)
		if err != nil {
			pqErr, ok := err.(*pq.Error)
			if ok {
				switch pqErr.Code.Name() {
				case "unique_violation":
					tx.Rollback()
					tx, err = db.BeginTx(ctx, nil)
					if err != nil {
						return nil, err
					}

					err := tx.QueryRowContext(
						ctx,
						selectOwnerQuery,
						item.Phone,
					).Scan(
						&item.Id,
						&item.FirstName,
						&item.SecondName,
						&item.Phone,
					)
					if err != nil {
						return nil, err
					}
					continue
				}
			}
			return nil, err
		}
	}

	// insert properties
	for _, item := range persons {
		amount, _ := strconv.ParseFloat(item.Amount, 64)
		id := strings.Split(uuid.New().String(), "-")

		_, err := tx.ExecContext(
			ctx,
			insertPropertyQuery,
			strings.ToUpper(id[0]),
			item.Id,
			amount,
			item.Sector,
			item.Cell,
			item.Village,
			item.RecordedBy,
			item.ForRent,
			item.NameSpace,
		)
		if err != nil {
			return nil, err
		}
	}

	return persons, tx.Commit()
}

// For inseting into persons into proterties
func (db *DB) insertPortperties(ctx context.Context, persons []*Person) ([]*Person, error) {

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	for _, item := range persons {
		amount, _ := strconv.ParseFloat(item.Amount, 64)
		_, err := tx.ExecContext(
			ctx,
			insertPropertyQuery,
			uuid.New().String(),
			item.Id,
			amount,
			item.Sector,
			item.Cell,
			item.Village,
			item.RecordedBy,
			item.ForRent,
			item.NameSpace,
		)
		if err != nil {
			return nil, err
		}
	}

	return persons, tx.Commit()
}

var inserOwnerQuery = `
INSERT INTO owners 
	(id, fname, lname, phone)
VALUES 
	($1, $2, $3, $4)
RETURNING id
`

var selectOwnerQuery = `
SELECT
	id, fname, lname, phone
FROM 
	owners
WHERE 
	phone = $1
`

var insertPropertyQuery = `
INSERT INTO properties(
		id,
		owner, 
		due, 
		sector, 
		cell, 
		village, 
		recorded_by, 
		occupied,
		namespace
	) 
VALUES 
	($1, $2, $3, $4, $5, $6, $7, $8, $9)
`
