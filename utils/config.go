package utils

import (
	"github.com/spf13/viper"
	"reflect"
)

type Config struct {
	GRPCServerAddress       string `mapstructure:"GRPC_SERVER_ADDRESS"`
	PrometheusMetricAddress string `mapstructure:"PROMETHEUS_METRIC_ADDRESS"`
	PGSource                string `mapstructure:"DB_SOURCE"`
	Storage                 string `mapstructure:"STORAGE"`
	TracerSource            string `mapstructure:"TRACER_SOURCE"`
	CassandraSource         string `mapstructure:"CASSANDRA_SOURCE"`
	CassandraKeySpace       string `mapstructure:"CASSANDRA_KEYSPACE"`
	CassandraPort           int    `mapstructure:"CASSANDRA_PORT"`
	WriteMode               string `mapstructure:"WRITE_MODE"`
	PGConnectionPoolSize    int    `mapstructure:"PG_CONNECTION_POOL_SIZE"`
	ProfilingServerAddress  string `mapstructure:"PROFILING_SERVER_ADDRESS"`
}

func setDefaults() {
	configType := reflect.TypeOf(Config{})
	for i := 0; i < configType.NumField(); i++ {
		field := configType.Field(i)
		tag := field.Tag.Get("mapstructure")
		viper.BindEnv(tag)
		viper.SetDefault(tag, "")
	}
}

func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")

	viper.AutomaticEnv()

	err = viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return
		} else {
			setDefaults()
		}
	}

	err = viper.Unmarshal(&config)
	return
}
