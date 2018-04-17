package corral

import (
	"github.com/spf13/viper"
)

func loadConfig() {
	viper.SetConfigName("corralrc")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.corral")

	setupDefaults()

	viper.ReadInConfig()

	viper.SetEnvPrefix("corral")
	viper.AutomaticEnv()
}

func setupDefaults() {
	defaultSettings := map[string]interface{}{
		"function_name":    "corral_function",
		"max_memory":       1500,
		"max_timeout":      180,
		"cleanup":          false,
		"verbose":          false,
		"split_size":       100 * 1024 * 1024, // Default input split size is 100Mb
		"map_bin_size":     512 * 1024 * 1024, // Default map bin size is 512Mb
		"reduce_bin_size":  512 * 1024 * 1024, // Default reduce bin size is 512Mb
		"max_concurrency":  500,               // Maximum number of concurrent executors
		"working_location": ".",
	}
	for key, value := range defaultSettings {
		viper.SetDefault(key, value)
	}

	aliases := map[string]string{
		"verbose":          "v",
		"working_location": "o",
	}
	for key, alias := range aliases {
		viper.RegisterAlias(alias, key)
	}
}
