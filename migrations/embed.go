// Package migrations embeds Anchor's SQL migrations into the binary so that
// anchorctl, the server and the worker all carry the schema they expect. There
// is no separate migration tool to install or keep in sync.
package migrations

import "embed"

// FS holds every .sql migration, named <version>_<name>.<up|down>.sql.
//
//go:embed *.sql
var FS embed.FS
