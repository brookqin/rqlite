// Command qiankun-baseline performs a host-local SQLite self-check for the
// controlled Qiankun rqlite downstream. It is not linked into production.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	qkdb "github.com/rqlite/rqlite/v10/db"
)

type report struct {
	SQLiteVersion      string   `json:"sqlite_version"`
	CompileOptions     []string `json:"compile_options"`
	WALCreated         bool     `json:"wal_created"`
	ForeignKeyEnforced bool     `json:"foreign_key_enforced"`
	BackupVerified     bool     `json:"backup_verified"`
}

func main() {
	tempDir, err := os.MkdirTemp("", "qiankun-rqlite-baseline-")
	if err != nil {
		fatalf("create temporary directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	databasePath := filepath.Join(tempDir, "source.db")
	database, err := qkdb.Open(databasePath, true, true)
	if err != nil {
		fatalf("open WAL database: %v", err)
	}
	defer database.Close()

	compileOptions, err := database.CompileOptions()
	if err != nil {
		fatalf("read SQLite compile options: %v", err)
	}

	executeOK(database, "CREATE TABLE parent (id INTEGER PRIMARY KEY)")
	executeOK(database, "CREATE TABLE child (parent_id INTEGER NOT NULL, FOREIGN KEY(parent_id) REFERENCES parent(id))")
	foreignKeyEnforced := executeHasError(database, "INSERT INTO child(parent_id) VALUES(1)")
	if !foreignKeyEnforced {
		fatalf("foreign key violation was accepted")
	}
	executeOK(database, "INSERT INTO parent(id) VALUES(1)")
	executeOK(database, "INSERT INTO child(parent_id) VALUES(1)")

	backupPath := filepath.Join(tempDir, "backup.db")
	if _, err := database.Backup(backupPath, false); err != nil {
		fatalf("backup database: %v", err)
	}
	backup, err := qkdb.Open(backupPath, true, false)
	if err != nil {
		fatalf("open backup database: %v", err)
	}
	rows, err := backup.QueryStringStmt("SELECT COUNT(*) FROM child")
	closeErr := backup.Close()
	if err != nil {
		fatalf("query backup database: %v", err)
	}
	if closeErr != nil {
		fatalf("close backup database: %v", closeErr)
	}
	backupVerified := len(rows) == 1 && len(rows[0].Values) == 1 &&
		len(rows[0].Values[0].Parameters) == 1 && rows[0].Values[0].Parameters[0].GetI() == 1
	if !backupVerified {
		fatalf("backup database did not preserve the expected row")
	}

	_, walErr := os.Stat(databasePath + "-wal")
	result := report{
		SQLiteVersion:      qkdb.DBVersion,
		CompileOptions:     compileOptions,
		WALCreated:         walErr == nil,
		ForeignKeyEnforced: foreignKeyEnforced,
		BackupVerified:     backupVerified,
	}
	if !result.WALCreated {
		fatalf("WAL file was not created: %v", walErr)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fatalf("encode report: %v", err)
	}
}

func executeOK(database *qkdb.DB, statement string) {
	responses, err := database.ExecuteStringStmt(statement)
	if err != nil {
		fatalf("execute %q: %v", statement, err)
	}
	if len(responses) != 1 {
		fatalf("execute %q returned %d responses", statement, len(responses))
	}
	if responseErr := responses[0].GetError(); responseErr != "" {
		fatalf("execute %q: %s", statement, responseErr)
	}
}

func executeHasError(database *qkdb.DB, statement string) bool {
	responses, err := database.ExecuteStringStmt(statement)
	if err != nil {
		fatalf("execute %q: %v", statement, err)
	}
	return len(responses) == 1 && responses[0].GetError() != ""
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "qiankun baseline: "+format+"\n", args...)
	os.Exit(1)
}
