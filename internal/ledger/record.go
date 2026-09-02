// Package ledger is base class #1 in Go form: a ClickHouse table is not a .sql
// file, it is a Record that knows its own columns, engine and ordering and
// generates its own DDL. cmd/chmigrate walks the registered Records and emits
// versioned migrations, so the schema can never drift from the code.
package ledger

import (
	"fmt"
	"strings"
)

// Column is one ClickHouse column. Declaration order is the column order, so
// Records carry a slice rather than a map.
type Column struct {
	Name string
	Type string
}

// Record is one table. Engine defaults to MergeTree; PartitionBy is optional.
type Record struct {
	Table       string
	Engine      string
	OrderBy     []string
	PartitionBy string
	Columns     []Column
}

// DDL renders the CREATE TABLE for this Record. It is deterministic: the same
// Record always renders byte-identical SQL, which is what lets the drift check
// compare generated output against committed migrations.
func (r Record) DDL() (string, error) {
	if r.Table == "" || len(r.Columns) == 0 {
		return "", fmt.Errorf("record %q: Table and Columns are required", r.Table)
	}

	cols := make([]string, len(r.Columns))
	for i, c := range r.Columns {
		if c.Name == "" || c.Type == "" {
			return "", fmt.Errorf("record %q: column %d has an empty name or type", r.Table, i)
		}
		cols[i] = fmt.Sprintf("  %s %s", c.Name, c.Type)
	}

	engine := r.Engine
	if engine == "" {
		engine = "MergeTree"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n%s\n)\n", r.Table, strings.Join(cols, ",\n"))
	fmt.Fprintf(&b, "ENGINE = %s\n", engine)
	if r.PartitionBy != "" {
		fmt.Fprintf(&b, "PARTITION BY %s\n", r.PartitionBy)
	}
	if len(r.OrderBy) > 0 {
		fmt.Fprintf(&b, "ORDER BY (%s)\n", strings.Join(r.OrderBy, ", "))
	}
	return strings.TrimRight(b.String(), "\n") + ";", nil
}

// registry holds every Record the binary knows about, in registration order.
var registry []Record

// Register adds a Record to the set the migrator walks. Call it from an init
// function in the file that declares the table.
func Register(r Record) {
	registry = append(registry, r)
}

// All returns the registered Records in declaration order.
func All() []Record {
	out := make([]Record, len(registry))
	copy(out, registry)
	return out
}
