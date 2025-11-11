package main

import (
	"fmt"
	"os"

	"github.com/BemiHQ/BemiDB/src/syncer-postgres/lib"
)

func init() {
	postgres.RegisterFlags()
}

func main() {
	// DISABLED: DuckLake integration does not require syncers
	// The DuckLake catalog is maintained externally
	// To re-enable: uncomment the code below and update for DuckLake compatibility

	fmt.Println("ERROR: syncer-postgres is disabled in DuckLake integration")
	fmt.Println("The DuckLake catalog is managed externally and does not require BemiDB syncers.")
	fmt.Println("If you need to sync data, use the external DuckLake syncing mechanism.")
	os.Exit(1)

	/*
	config := postgres.LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	syncer := postgres.NewSyncer(config)
	syncer.Sync()
	*/
}
