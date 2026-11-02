package cmd

import (
	"github.com/iobestar/sensus/internal/server"
	"github.com/spf13/cobra"
)

func newServerCmd() *baseCmd {
	return newBaseCmd(&cobra.Command{
		Use:   "server",
		Short: "Start server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return server.Start(cmd, Version, Build)
		},
	})
}
