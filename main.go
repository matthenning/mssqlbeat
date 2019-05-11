package main

import (
	"os"

	"github.com/mathenning/mssqlbeat/cmd"

	_ "github.com/mathenning/mssqlbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
