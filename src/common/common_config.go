package common

const (
	VERSION = "1.7.0"

	ENV_LOG_LEVEL                   = "BEMIDB_LOG_LEVEL"
	ENV_DISABLE_ANONYMOUS_ANALYTICS = "BEMIDB_DISABLE_ANONYMOUS_ANALYTICS"

	ENV_CATALOG_DATABASE_URL = "CATALOG_DATABASE_URL"

	ENV_AWS_REGION            = "AWS_REGION"
	ENV_AWS_S3_ENDPOINT       = "AWS_S3_ENDPOINT"
	ENV_AWS_S3_BUCKET         = "AWS_S3_BUCKET"
	ENV_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	ENV_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"

	// DuckLake environment variables
	ENV_DUCKLAKE_CATALOG_URL  = "DUCKLAKE_CATALOG_URL"
	ENV_DUCKLAKE_CATALOG_NAME = "DUCKLAKE_CATALOG_NAME"
	ENV_DUCKLAKE_DATA_PATH    = "DUCKLAKE_DATA_PATH"

	// R2 environment variables
	ENV_R2_ACCOUNT_ID        = "R2_ACCOUNT_ID"
	ENV_R2_ACCESS_KEY_ID     = "R2_ACCESS_KEY_ID"
	ENV_R2_SECRET_ACCESS_KEY = "R2_SECRET_ACCESS_KEY"

	// Performance and resource configuration
	ENV_MEMORY_LIMIT      = "BEMIDB_MEMORY_LIMIT"
	ENV_TEMP_DIRECTORY    = "BEMIDB_TEMP_DIRECTORY"
	ENV_QUERY_TIMEOUT     = "BEMIDB_QUERY_TIMEOUT"
	ENV_THREADS           = "BEMIDB_THREADS"

	DEFAULT_LOG_LEVEL             = "INFO"
	DEFAULT_AWS_S3_ENDPOINT       = "s3.amazonaws.com"
	DEFAULT_DUCKLAKE_CATALOG_NAME = "ducklake"
	DEFAULT_MEMORY_LIMIT          = "3GB"
	DEFAULT_QUERY_TIMEOUT         = 300 // 5 minutes in seconds
)

type AwsConfig struct {
	Region          string
	S3Endpoint      string // optional
	S3Bucket        string
	AccessKeyId     string
	SecretAccessKey string
}

type DucklakeConfig struct {
	CatalogUrl  string
	CatalogName string
	DataPath    string
}

type R2Config struct {
	AccountId       string
	AccessKeyId     string
	SecretAccessKey string
}

type CommonConfig struct {
	Aws                       AwsConfig      // DEPRECATED: Not used with DuckLake
	Ducklake                  DucklakeConfig
	R2                        R2Config
	LogLevel                  string
	CatalogDatabaseUrl        string // DEPRECATED: Not used with DuckLake
	DisableAnonymousAnalytics bool
	// Performance and resource configuration
	MemoryLimit      string // DuckDB memory limit (e.g., '3GB', '8GB')
	TempDirectory    string // DuckDB temp directory for spill-to-disk
	QueryTimeout     int    // Query timeout in seconds
	Threads          int    // Number of DuckDB threads (0 = auto-detect)
}
