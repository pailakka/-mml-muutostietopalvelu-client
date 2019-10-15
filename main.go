package main

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func main() {

	app := cli.NewApp()
	app.Name = "mml-muutostietopalvelu-client"
	app.Version = "0.1"
	app.Usage = "Loads and keeps datasets up to date from NLS open data atom feed"

	app.Commands = []cli.Command{
		{
			Name:         "load",
			ShortName:    "",
			Aliases:      nil,
			Usage:        "load the whole product",
			UsageText:    "",
			Description:  "",
			ArgsUsage:    "",
			Category:     "",
			BashComplete: nil,
			Before:       nil,
			After:        nil,
			Action: func(c *cli.Context) error {
				product := c.String("product")
				version := c.String("type")
				format := c.String("format")
				dest := c.String("destination")
				fromdate := c.String("fromdate")
				force := c.Bool("force")
				onlymissing := c.Bool("missing")

				if product == "" {
					panic("Product required")
				}

				if version == "" {
					panic("Version required")
				}

				if format == "" {
					panic("Format required")
				}

				if dest == "" {
					panic("Dest required")
				}

				if fromdate == "" {
					fromdate = "1990-06-20T00:00:00"
				}

				_ = os.MkdirAll(dest, 0755)

				err := loadProductToDest(product, version, format, dest, force, onlymissing, fromdate)
				if err != nil {
					panic(err)
				}
				fmt.Println(product)

				return nil
			},
			OnUsageError: nil,
			Subcommands:  nil,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "product, p",
					Usage: "product to be processed",
				},
				cli.StringFlag{
					Name:  "type, t",
					Usage: "product version to be processed",
				},
				cli.StringFlag{
					Name:  "format, f",
					Usage: "file format to request",
				},
				cli.StringFlag{
					Name:  "destination, d",
					Usage: "destination path",
				},
				cli.StringFlag{
					Name:  "fromdate, fd",
					Usage: "load from date. Format: 2019-03-22T00:00:00Z",
				},
				cli.BoolFlag{
					Name:  "force",
					Usage: "force load all items",
				},
				cli.BoolFlag{
					Name:  "missing",
					Usage: "load only missing items",
				},
			},
			SkipFlagParsing:    false,
			SkipArgReorder:     false,
			HideHelp:           false,
			Hidden:             false,
			HelpName:           "",
			CustomHelpTemplate: "",
		},
		{
			Name:  "list",
			Usage: "list available products",
			Action: func(c *cli.Context) error {
				log.Info("Listing available products")

				products, err := loadProductsList()

				if err != nil {
					panic(err)
				}

				for _, p := range products {
					fmt.Printf("%s\t%s\n", p.Updated, p.Title)
					fmt.Println(p.GUID)
					for _, f := range p.Formats {
						fmt.Printf("\t%s\n", f)
					}
					fmt.Print("\n")
				}
				return nil
			},
		},
	}
	err := app.Run(os.Args)

	if err != nil {
		panic(err)
	}
}
