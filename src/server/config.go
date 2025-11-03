package main

import (
	"flag"
	"os"
	"slices"
	"strings"

	"github.com/BemiHQ/BemiDB/src/common"
)

const (
	ENV_PORT            = "BEMIDB_PORT"
	ENV_DATABASE        = "BEMIDB_DATABASE"
	ENV_USER            = "BEMIDB_USER"
	ENV_PASSWORD        = "BEMIDB_PASSWORD"
	ENV_HOST            = "BEMIDB_HOST"
	ENV_MAX_CONNECTIONS = "BEMIDB_MAX_CONNECTIONS"

	DEFAULT_LOG_LEVEL       = "INFO"
	DEFAULT_HOST            = "0.0.0.0"
	DEFAULT_PORT            = "54321"
	DEFAULT_DATABASE        = "bemidb"
	DEFAULT_MAX_CONNECTIONS = 100
	DEFAULT_AWS_S3_ENDPOINT = "s3.amazonaws.com"
)

type Config struct {
	CommonConfig      *common.CommonConfig
	Host              string
	Port              string
	Database          string
	User              string
	EncryptedPassword string
	MaxConnections    int
}

type configParseValues struct {
	password string
}

var _config Config
var _configParseValues configParseValues

func init() {
	registerFlags()
}

func registerFlags() {
	_config.CommonConfig = &common.CommonConfig{}

	flag.StringVar(&_config.CommonConfig.LogLevel, "log-level", os.Getenv(common.ENV_LOG_LEVEL), `Log level: "ERROR", "WARN", "INFO", "DEBUG", "TRACE". Default: "`+common.DEFAULT_LOG_LEVEL+`"`)
	flag.StringVar(&_config.CommonConfig.CatalogDatabaseUrl, "catalog-database-url", os.Getenv(common.ENV_CATALOG_DATABASE_URL), "Catalog database URL")
	flag.StringVar(&_config.CommonConfig.Aws.Region, "aws-region", os.Getenv(common.ENV_AWS_REGION), "AWS region")
	flag.StringVar(&_config.CommonConfig.Aws.S3Endpoint, "aws-s3-endpoint", os.Getenv(common.ENV_AWS_S3_ENDPOINT), "AWS S3 endpoint. Default: \""+common.DEFAULT_AWS_S3_ENDPOINT+`"`)
	flag.StringVar(&_config.CommonConfig.Aws.S3Bucket, "aws-s3-bucket", os.Getenv(common.ENV_AWS_S3_BUCKET), "AWS S3 bucket name")
	flag.StringVar(&_config.CommonConfig.Aws.AccessKeyId, "aws-access-key-id", os.Getenv(common.ENV_AWS_ACCESS_KEY_ID), "AWS access key ID")
	flag.StringVar(&_config.CommonConfig.Aws.SecretAccessKey, "aws-secret-access-key", os.Getenv(common.ENV_AWS_SECRET_ACCESS_KEY), "AWS secret access key")
	flag.BoolVar(&_config.CommonConfig.DisableAnonymousAnalytics, "disable-anonymous-analytics", os.Getenv(common.ENV_DISABLE_ANONYMOUS_ANALYTICS) == "true", "Disable anonymous analytics collection")

	// DuckLake configuration
	flag.StringVar(&_config.CommonConfig.Ducklake.CatalogUrl, "ducklake-catalog-url", os.Getenv(common.ENV_DUCKLAKE_CATALOG_URL), "DuckLake catalog database URL (PostgreSQL connection string)")
	flag.StringVar(&_config.CommonConfig.Ducklake.CatalogName, "ducklake-catalog-name", os.Getenv(common.ENV_DUCKLAKE_CATALOG_NAME), "DuckLake catalog name. Default: \""+common.DEFAULT_DUCKLAKE_CATALOG_NAME+"\"")
	flag.StringVar(&_config.CommonConfig.Ducklake.DataPath, "ducklake-data-path", os.Getenv(common.ENV_DUCKLAKE_DATA_PATH), "DuckLake data path in R2 storage")

	// R2 configuration
	flag.StringVar(&_config.CommonConfig.R2.AccountId, "r2-account-id", os.Getenv(common.ENV_R2_ACCOUNT_ID), "R2 account ID")
	flag.StringVar(&_config.CommonConfig.R2.AccessKeyId, "r2-access-key-id", os.Getenv(common.ENV_R2_ACCESS_KEY_ID), "R2 access key ID")
	flag.StringVar(&_config.CommonConfig.R2.SecretAccessKey, "r2-secret-access-key", os.Getenv(common.ENV_R2_SECRET_ACCESS_KEY), "R2 secret access key")

	// Performance and resource configuration
	flag.StringVar(&_config.CommonConfig.MemoryLimit, "memory-limit", os.Getenv(common.ENV_MEMORY_LIMIT), "DuckDB memory limit (e.g., '3GB', '8GB'). Default: \""+common.DEFAULT_MEMORY_LIMIT+"\"")
	flag.StringVar(&_config.CommonConfig.TempDirectory, "temp-directory", os.Getenv(common.ENV_TEMP_DIRECTORY), "DuckDB temp directory for spill-to-disk")
	flag.IntVar(&_config.CommonConfig.QueryTimeout, "query-timeout", 0, "Query timeout in seconds. Default: "+common.IntToString(common.DEFAULT_QUERY_TIMEOUT))
	flag.IntVar(&_config.CommonConfig.Threads, "threads", 0, "Number of DuckDB threads (0 = auto-detect)")

	flag.StringVar(&_config.Host, "host", os.Getenv(ENV_HOST), "Host for BemiDB to listen on")
	flag.StringVar(&_config.Port, "port", os.Getenv(ENV_PORT), "Port for BemiDB to listen on")
	flag.StringVar(&_config.Database, "database", os.Getenv(ENV_DATABASE), "Database name")
	flag.StringVar(&_config.User, "user", os.Getenv(ENV_USER), "Database user")
	flag.StringVar(&_configParseValues.password, "password", os.Getenv(ENV_PASSWORD), "Database password")
	flag.IntVar(&_config.MaxConnections, "max-connections", 0, "Maximum concurrent connections (0 = unlimited). Default: "+common.IntToString(DEFAULT_MAX_CONNECTIONS))
}

func parseFlags() {
	flag.Parse()

	if _config.CommonConfig.LogLevel == "" {
		_config.CommonConfig.LogLevel = common.DEFAULT_LOG_LEVEL
	} else if !slices.Contains(common.LOG_LEVELS, _config.CommonConfig.LogLevel) {
		panic("Invalid log level " + _config.CommonConfig.LogLevel + ". Must be one of " + strings.Join(common.LOG_LEVELS, ", "))
	}

	// DuckLake configuration takes precedence over legacy Iceberg configuration
	if _config.CommonConfig.Ducklake.CatalogUrl != "" {
		// Using DuckLake - validate DuckLake configuration
		if _config.CommonConfig.Ducklake.CatalogName == "" {
			_config.CommonConfig.Ducklake.CatalogName = common.DEFAULT_DUCKLAKE_CATALOG_NAME
		}
		if _config.CommonConfig.Ducklake.DataPath == "" {
			panic("DuckLake data path is required when using DuckLake")
		}
		if _config.CommonConfig.R2.AccountId == "" {
			panic("R2 account ID is required when using DuckLake")
		}
		if _config.CommonConfig.R2.AccessKeyId == "" {
			panic("R2 access key ID is required when using DuckLake")
		}
		if _config.CommonConfig.R2.SecretAccessKey == "" {
			panic("R2 secret access key is required when using DuckLake")
		}
	} else {
		// Using legacy Iceberg configuration
		if _config.CommonConfig.CatalogDatabaseUrl == "" {
			panic("Catalog database URL is required")
		}
		if _config.CommonConfig.Aws.Region == "" {
			panic("AWS region is required")
		}
		if _config.CommonConfig.Aws.S3Endpoint == "" {
			_config.CommonConfig.Aws.S3Endpoint = common.DEFAULT_AWS_S3_ENDPOINT
		}
		if _config.CommonConfig.Aws.S3Bucket == "" {
			panic("AWS S3 bucket name is required")
		}
		if _config.CommonConfig.Aws.AccessKeyId != "" && _config.CommonConfig.Aws.SecretAccessKey == "" {
			panic("AWS secret access key is required")
		}
		if _config.CommonConfig.Aws.AccessKeyId == "" && _config.CommonConfig.Aws.SecretAccessKey != "" {
			panic("AWS access key ID is required")
		}
	}

	if _config.Host == "" {
		_config.Host = DEFAULT_HOST
	}
	if _config.Port == "" {
		_config.Port = DEFAULT_PORT
	}
	if _config.Database == "" {
		_config.Database = DEFAULT_DATABASE
	}
	if _config.MaxConnections == 0 {
		// Check environment variable
		if maxConnEnv := os.Getenv(ENV_MAX_CONNECTIONS); maxConnEnv != "" {
			if maxConn := common.StringToInt(maxConnEnv); maxConn > 0 {
				_config.MaxConnections = maxConn
			} else {
				_config.MaxConnections = DEFAULT_MAX_CONNECTIONS
			}
		} else {
			_config.MaxConnections = DEFAULT_MAX_CONNECTIONS
		}
	}

	// Set defaults for performance configuration
	if _config.CommonConfig.QueryTimeout == 0 {
		// Check environment variable
		if timeoutEnv := os.Getenv(common.ENV_QUERY_TIMEOUT); timeoutEnv != "" {
			if timeout := common.StringToInt(timeoutEnv); timeout > 0 {
				_config.CommonConfig.QueryTimeout = timeout
			} else {
				_config.CommonConfig.QueryTimeout = common.DEFAULT_QUERY_TIMEOUT
			}
		} else {
			_config.CommonConfig.QueryTimeout = common.DEFAULT_QUERY_TIMEOUT
		}
	}

	if _config.CommonConfig.Threads == 0 {
		// Check environment variable (0 means auto-detect, so only override if explicitly set)
		if threadsEnv := os.Getenv(common.ENV_THREADS); threadsEnv != "" {
			_config.CommonConfig.Threads = common.StringToInt(threadsEnv)
		}
	}

	if _configParseValues.password != "" {
		_config.EncryptedPassword = StringToScramSha256(_configParseValues.password)
	}

	_configParseValues = configParseValues{}
}

func LoadConfig() *Config {
	parseFlags()
	return &_config
}
