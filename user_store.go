package main

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
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

	for _, person := range persons {
		var id string

		err = tx.QueryRowContext(ctx, "SELECT id FROM owners WHERE phone = $1", person.Phone).Scan(&id)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if err == sql.ErrNoRows {
			// Insert new row into owner table
			id = uuid.New().String()
			_, err = tx.ExecContext(
				ctx,
				inserOwnerQuery,
				id,
				person.FirstName,
				person.SecondName,
				person.Phone,
			)
			if err != nil {
				return nil, err
			}
		}
		person.Id = id
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

	pos, args := []string{}, []interface{}{}

	i := 0
	for _, p := range persons {
		amount, _ := strconv.ParseFloat(p.Amount, 64)
		id := strings.Split(uuid.New().String(), "-")
		pos = append(pos,
			fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*9+1, i*9+2, i*9+3, i*9+4, i*9+5, i*9+6, i*9+7, i*9+8, i*9+9),
		)
		args = append(
			args,
			strings.ToUpper(id[0]),
			p.Id,
			amount,
			p.Sector,
			p.Cell,
			p.Village,
			p.RecordedBy,
			p.ForRent,
			p.NameSpace,
		)
		i++
	}

	var query = insertPropertyQuery + strings.Join(pos, ",")

	_, err = tx.ExecContext(
		ctx,
		query,
		args...,
	)

	if err != nil {
		return nil, err
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
`
