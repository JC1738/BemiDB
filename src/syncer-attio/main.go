package main

import (
	"fmt"
	"os"

	"github.com/BemiHQ/BemiDB/src/syncer-attio/lib"
)

func init() {
	attio.RegisterFlags()
}

func main() {
	// DISABLED: DuckLake integration does not require syncers
	fmt.Println("ERROR: syncer-attio is disabled in DuckLake integration")
	fmt.Println("The DuckLake catalog is managed externally and does not require BemiDB syncers.")
	os.Exit(1)

	/*
	config := attio.LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	storageS3 := common.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, common.SYNCER_DUCKDB_BOOT_QUERIES)
	syncer := attio.NewSyncer(config, storageS3, duckdbClient)
	syncer.Sync()
	*/
}
