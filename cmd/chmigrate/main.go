// Command chmigrate manages the ClickHouse schema.
//
//	chmigrate emit -name add_test_campaign   write a migration for pending Records
//	chmigrate check                          fail if a Record has no migration (CI gate)
//	chmigrate apply                          apply unapplied migrations to the database
//
// Connection facts come from the environment and nothing is baked in: a
// missing one is a loud error, never a silent localhost.
//
//	CH_HOST CH_PORT CH_DATABASE CH_USER CH_PASSWORD
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/michaeltabet/ingest/internal/migrate"

	_ "github.com/michaeltabet/ingest/internal/ledger"
)

const migrationsDir = "clickhouse/migrations"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "chmigrate:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: chmigrate <emit|check|apply> [flags]")
	}

	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	dir := fs.String("dir", migrationsDir, "migrations directory")
	name := fs.String("name", "", "slug for the emitted migration (emit only)")
	if err := fs.Parse(os.Args[2:]); err != nil {
		return err
	}

	switch os.Args[1] {
	case "emit":
		if *name == "" {
			return fmt.Errorf("emit needs -name, e.g. -name add_test_campaign")
		}
		path, err := migrate.Emit(*dir, *name)
		if err != nil {
			return err
		}
		if path == "" {
			fmt.Println("schema is current — nothing to emit")
			return nil
		}
		fmt.Println("wrote", path)
		return nil

	case "check":
		pending, err := migrate.Pending(*dir)
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			fmt.Println("no drift — every Record has a migration")
			return nil
		}
		for _, r := range pending {
			fmt.Fprintf(os.Stderr, "  no migration creates %s\n", r.Table)
		}
		return fmt.Errorf("%d record(s) have no migration — run: chmigrate emit -name <slug>", len(pending))

	case "apply":
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		conn, err := connect()
		if err != nil {
			return err
		}
		defer conn.Close()

		ran, err := migrate.Apply(ctx, conn, *dir)
		for _, f := range ran {
			fmt.Println("applied", f)
		}
		if err != nil {
			return err
		}
		if len(ran) == 0 {
			fmt.Println("database is up to date")
		}
		return nil

	default:
		return fmt.Errorf("unknown command %q — expected emit, check or apply", os.Args[1])
	}
}

func connect() (clickhouse.Conn, error) {
	env := func(key string) (string, error) {
		v := os.Getenv(key)
		if v == "" {
			return "", fmt.Errorf("%s is not set — the migrator bakes in nothing", key)
		}
		return v, nil
	}

	host, err := env("CH_HOST")
	if err != nil {
		return nil, err
	}
	port, err := env("CH_PORT")
	if err != nil {
		return nil, err
	}
	database, err := env("CH_DATABASE")
	if err != nil {
		return nil, err
	}
	user, err := env("CH_USER")
	if err != nil {
		return nil, err
	}

	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{host + ":" + port},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: os.Getenv("CH_PASSWORD"),
		},
	})
}
