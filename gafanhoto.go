package database

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
)

var db *sql.DB

func AbrirConexao(nome string, url string) {
	stdlib.RegisterConnConfig(&pgx.ConnConfig{
		Config: pgconn.Config{
			RuntimeParams: map[string]string{
				"application_name": nome,
				"DateStyle":        "ISO",
				"IntervalStyle":    "iso_8601",
				"search_path":      "public",
			},
			AfterConnect: func(ctx context.Context, pgconn *pgconn.PgConn) error {
				log.Println("Conectado!!")
				return nil
			},
		},
	})

	var erro error
	if db, erro = sql.Open("pgx", url); erro != nil {
		panic(erro)
	}

	if erro = db.Ping(); erro != nil {
		panic(erro)
	}
}

func FecharConexao() {
	if db != nil {
		db.Close()
	}
}

type Tx struct {
	db *sql.Tx
}

func NovaTransacao(ctx context.Context, leitura bool) (*Tx, error) {
	var (
		tx   *sql.Tx
		erro error
	)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-time.After(time.Minute)
		if tx == nil {
			cancel()
		}
	}()

	if tx, erro = db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  leitura,
	}); erro != nil {
		return nil, erro
	}

	return &Tx{tx}, erro
}

func (tx *Tx) Rollback() {
	_ = tx.db.Rollback()
}

func (tx *Tx) Commit() error {
	return tx.db.Commit()
}

func (tx *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.db.Query(query, args...)
}

func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.db.Exec(query, args...)
}
