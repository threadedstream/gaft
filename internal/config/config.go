package config

import (
	"gopkg.in/yaml.v3"
	"os"
)

type BootstrapConfig struct {
	ServersInCluster []int `yaml:"servers_in_cluster"`
}

func Parse(filename string) (config BootstrapConfig, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	decoder := yaml.NewDecoder(file)
	if err = decoder.Decode(&config); err != nil {
		return config, err
	}
	return config, nil
}
