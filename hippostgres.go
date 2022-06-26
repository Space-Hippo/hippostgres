package hippostgres

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
)

func PostgresUpdateColumnDataOneRow(db *sql.DB, query string, params ...interface{}) error {
	log.Printf("Query: %s", query)
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Println(err)
		return err
	}
	results, err := stmt.Exec(params...)
	err = getRowsAffected(results, 1)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func PostgresScanOneRow(db *sql.DB, query string, params ...interface{}) (*sql.Row, error) {
	if len(params) < 1 {
		noParamsErr := errors.New("no params were passed")
		return nil, noParamsErr
	}
	log.Println("Query", query)
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	row := stmt.QueryRow(params...)
	return row, nil
}

func PostgresScanRows(db *sql.DB, query string, params ...interface{}) (*sql.Rows, error) {
	if len(params) < 1 {
		noParamsErr := errors.New("no params were passed")
		return nil, noParamsErr
	}
	log.Printf("Query %v", query)
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(params...)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return rows, nil
}

// Non-exported helper funcs

func getRowsAffected(results sql.Result, targetNumRowsAffected int64) error {
	rowsAffected, err := results.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != targetNumRowsAffected {
		sqlErr := errors.New(fmt.Sprintf("number of rows affected does not match the expected number of rows affected: %v / %v", rowsAffected, targetNumRowsAffected))
		log.Println(sqlErr)
		return sqlErr
	}
	log.Printf("Rows affected: %v / %v", rowsAffected, targetNumRowsAffected)
	return nil
}
