// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period   time.Duration `config:"period"`
	Username string        `config:"username"`
	Password string        `config:"password"`
	Host     string        `config:"host"`
	Instance string        `config:"instance"`
	Port     int           `config:"port"`
}

var DefaultConfig = Config{
	Period:   1 * time.Second,
	Instance: "",
	Port:     1433,
}
