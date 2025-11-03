package main

import (
	"fmt"
	"os"

	"github.com/BemiHQ/BemiDB/src/syncer-amplitude/lib"
)

func init() {
	amplitude.RegisterFlags()
}

func main() {
	// DISABLED: DuckLake integration does not require syncers
	fmt.Println("ERROR: syncer-amplitude is disabled in DuckLake integration")
	fmt.Println("The DuckLake catalog is managed externally and does not require BemiDB syncers.")
	os.Exit(1)

	/*
	config := amplitude.LoadConfig()
	defer common.HandleUnexpectedPanic(config.CommonConfig)

	storageS3 := common.NewStorageS3(config.CommonConfig)
	duckdbClient := common.NewDuckdbClient(config.CommonConfig, common.SYNCER_DUCKDB_BOOT_QUERIES)
	syncer := amplitude.NewSyncer(config, storageS3, duckdbClient)
	syncer.Sync()
	*/
}
