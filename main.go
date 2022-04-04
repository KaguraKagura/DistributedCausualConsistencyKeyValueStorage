package main

import (
	"fmt"
	"log"
	"os"

	"Lab2/client"
	"Lab2/server"
	"Lab2/util"

	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.App{
		Name:      util.AppName,
		Usage:     "a distributed key-value data center following causal consistency",
		UsageText: fmt.Sprintf("%s command", util.AppName),
		Commands: []*cli.Command{
			{
				Name:  "client",
				Usage: "Run in client mode",
				Action: func(context *cli.Context) error {
					client.Start()
					return nil
				},
			},
			{
				Name:  "server",
				Usage: "Run in server mode",
				Action: func(context *cli.Context) error {
					server.Start()
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
