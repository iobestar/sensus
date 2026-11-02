package main

import (
	"github.com/iobestar/sensus/cmd"
	"github.com/iobestar/sensus/internal"
)

func main() {
	if err := cmd.Execute(); nil != err {
		internal.FatalError(err)
	}
}
