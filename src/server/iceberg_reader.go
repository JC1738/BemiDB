package main

import (
	"github.com/BemiHQ/BemiDB/src/common"
)

type IcebergReader struct {
	Config         *Config
	IcebergCatalog interface{} // Supports both *common.IcebergCatalog and *common.DucklakeCatalog
}

func NewIcebergReader(config *Config, catalog interface{}) *IcebergReader {
	return &IcebergReader{
		Config:         config,
		IcebergCatalog: catalog,
	}
}

func (reader *IcebergReader) SchemaTables() (icebergSchemaTables common.Set[common.IcebergSchemaTable], err error) {
	switch catalog := reader.IcebergCatalog.(type) {
	case *common.IcebergCatalog:
		return catalog.SchemaTables()
	case *common.DucklakeCatalog:
		// DucklakeCatalog.SchemaTables() requires a DuckdbClient, but we don't have it here
		// This should not be called for DuckLake - reloadDucklakeTables() is used instead
		return common.NewSet[common.IcebergSchemaTable](), nil
	default:
		return common.NewSet[common.IcebergSchemaTable](), nil
	}
}

func (reader *IcebergReader) MaterializedViews() (icebergSchemaTables []common.IcebergMaterializedView, err error) {
	switch catalog := reader.IcebergCatalog.(type) {
	case *common.IcebergCatalog:
		return catalog.MaterializedViews()
	case *common.DucklakeCatalog:
		return catalog.MaterializedViews()
	default:
		return []common.IcebergMaterializedView{}, nil
	}
}

func (reader *IcebergReader) MaterializedView(icebergSchemaTable common.IcebergSchemaTable) (icebergMaterializedView common.IcebergMaterializedView, err error) {
	if catalog, ok := reader.IcebergCatalog.(*common.IcebergCatalog); ok {
		return catalog.MaterializedView(icebergSchemaTable)
	}
	// DuckLake doesn't support this
	return common.IcebergMaterializedView{}, nil
}

func (reader *IcebergReader) TableColumns(icebergSchemaTable common.IcebergSchemaTable) (catalogTableColumns []common.CatalogTableColumn, err error) {
	switch catalog := reader.IcebergCatalog.(type) {
	case *common.IcebergCatalog:
		return catalog.TableColumns(icebergSchemaTable)
	case *common.DucklakeCatalog:
		return catalog.TableColumns(icebergSchemaTable)
	default:
		return []common.CatalogTableColumn{}, nil
	}
}

func (reader *IcebergReader) MetadataFileS3Path(icebergSchemaTable common.IcebergSchemaTable) string {
	switch catalog := reader.IcebergCatalog.(type) {
	case *common.IcebergCatalog:
		return catalog.MetadataFileS3Path(icebergSchemaTable)
	case *common.DucklakeCatalog:
		return catalog.MetadataFileS3Path(icebergSchemaTable)
	default:
		return ""
	}
}
