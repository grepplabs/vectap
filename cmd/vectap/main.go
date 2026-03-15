package main

import (
	"os"

	"github.com/grepplabs/vectap/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
