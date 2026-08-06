// Package migrations embeds the SQL schema migrations so that every binary
// ships with the exact schema it was built against.
package migrations

import "embed"

// FS holds the .sql migration files, named <version>_<name>.(up|down).sql.
//
//go:embed *.sql
var FS embed.FS
