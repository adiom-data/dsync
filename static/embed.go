// static.go
package static

import "embed"

//go:embed *
var WebStatic embed.FS
