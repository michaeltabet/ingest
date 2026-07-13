# Fixture provenance — real vs synthetic

A fixture is only trustworthy if it was RECORDED from a live board via
`python -m ingest.cli record <platform> --slug <slug>`. Synthetic (hand-written)
fixtures prove parsing logic only and MUST be replaced before that platform is
considered ported.

| Platform | Status | Source |
|---|---|---|
| greenhouse | ✅ RECORDED | live board `gitlab`, 155 jobs, 2026-07-13 |
| ashby | ✅ RECORDED | live board `openai`, 728 jobs, 2026-07-13 |
| lever | ✅ RECORDED | live board `highspot`, 18 jobs, 2026-07-13 |
| workable | ⚠️ SYNTHETIC | hand-written shape; no live slug found yet — REPLACE |
| bamboohr | ⚠️ SYNTHETIC | hand-written shape; list endpoint 302s on probed slugs — REPLACE (endpoint may need cookies/headers: verify against atlas-kt runtime behavior) |

Rule: a platform with SYNTHETIC fixtures cannot pass the port gate (docs/BUILD-BRIEF.md §5.5).
