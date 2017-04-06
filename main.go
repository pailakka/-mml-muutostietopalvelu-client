package main

import (
	"fmt"
	"os"

	"github.com/inconshreveable/log15"
	"github.com/urfave/cli"
)

func main() {

	app := cli.NewApp()
	app.Name = "mml-muutostietopalvelu-client"
	app.Version = "0.1"
	app.Usage = "Loads and keeps datasets up to date from NLS open data atom feed"

	app.Commands = []cli.Command{
		{
			Name:  "load",
			Usage: "load the whole product",
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
				cli.BoolFlag{
					Name:  "force",
					Usage: "force load all items",
				},
			},
			Action: func(c *cli.Context) error {
				product := c.String("product")
				version := c.String("type")
				format := c.String("format")
				dest := c.String("destination")
				force := c.Bool("force")

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

				os.MkdirAll(dest, 0755)

				loadProductToDest(product, version, format, dest, force)

				fmt.Println(product)

				return nil
			},
		},
		{
			Name:  "list",
			Usage: "list available products",
			Action: func(c *cli.Context) error {
				log15.Info("Listing available products")

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
	app.Run(os.Args)

}
