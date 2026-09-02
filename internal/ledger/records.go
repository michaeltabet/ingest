package ledger

// The tables. Each one is declared here and nowhere else — these two were
// previously hand-written _DDL strings applied at runtime by test.py and
// janitor.py, which is exactly the drift the Record design forbids.

func init() {
	Register(Record{
		Table:   "test_campaign",
		Engine:  "MergeTree",
		OrderBy: []string{"platform", "run_at"},
		Columns: []Column{
			{"campaign", "String"},
			{"platform", "LowCardinality(String)"},
			{"key", "String"},
			{"rung", "UInt16"},
			{"passed", "UInt8"},
			{"evidence_outcome", "LowCardinality(String)"},
			{"data_errors", "Array(String)"},
			{"params", "String"},
			{"run_at", "DateTime64(3)"},
		},
	})

	Register(Record{
		Table:   "calibration_observations",
		Engine:  "MergeTree",
		OrderBy: []string{"platform", "run_at"},
		Columns: []Column{
			{"platform", "LowCardinality(String)"},
			{"window_closed", "UInt32"},
			{"sources_measured", "UInt32"},
			{"avg_seconds", "Float64"},
			{"ok_rate", "Float64"},
			{"recommend_batch", "UInt16"},
			{"note", "String"},
			{"run_at", "DateTime64(3)"},
		},
	})
}
