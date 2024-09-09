package utils

import (
	"github.com/spf13/viper"
	"reflect"
)

type Config struct {
	GRPCServerAddress       string `mapstructure:"GRPC_SERVER_ADDRESS"`
	PrometheusMetricAddress string `mapstructure:"PROMETHEUS_METRIC_ADDRESS"`
	DBSource                string `mapstructure:"DB_SOURCE"`
	Storage                 string `mapstructure:"STORAGE"`
	TracerSource            string `mapstructure:"TRACER_SOURCE"`
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
