package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

type DB struct {
	db *sql.DB

	ctx    context.Context // background context
	cancel func()          // cancel background context

	// Datasource name.
	DSN string

	// Returns the current time. Defaults to time.Now().
	// Can be mocked for tests.
	Now func() time.Time
}

// New ...
func New(ctx context.Context, dsn string) *DB {
	db := &DB{
		DSN: dsn,
		Now: time.Now,
	}

	db.ctx, db.cancel = context.WithCancel(ctx)
	return db
}

// Open a connection on the underlying server
func (db *DB) Open() (err error) {
	// Ensure a DSN is set before attempting to open the database.
	if db.DSN == "" {
		return fmt.Errorf("dsn required")
	}

	log.Println("opening database connection")

	db.db, err = sql.Open("postgres", db.DSN)
	if err != nil {
		return err
	}

	for i := 1; i < 10; i++ {
		time.Sleep(5 * time.Millisecond)

		if err == nil {
			log.Printf("pinging %d", i)

			err := db.Ping()
			if err == nil {
				return nil
			}
		}
	}

	db.db.SetMaxOpenConns(60)
	db.db.SetMaxIdleConns(30)
	db.db.SetConnMaxLifetime(15 * time.Minute)

	return fmt.Errorf("failed to connect to database")
}

func (db *DB) Close() {
	db.cancel()
	db.db.Close()
}

func (db *DB) Ping() error {
	return db.db.Ping()
}

// Tx wraps the SQL Tx object to provide a timestamp at the start of the transaction.
type Tx struct {
	*sql.Tx
	db  *DB
	Now time.Time
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Return wrapper Tx that includes the transaction start time.
	return &Tx{
		Tx:  tx,
		db:  db,
		Now: db.Now().UTC().Truncate(time.Second),
	}, nil
}
