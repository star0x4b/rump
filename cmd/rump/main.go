package main

import (
	"github.com/star0x4b/rump/pkg/config"
	"github.com/star0x4b/rump/pkg/run"
)

func main() {
	// parse config flags, will exit in case of errors.
	cfg := config.Parse()

	run.Run(cfg)
}
