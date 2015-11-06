package main

import (
	"io/ioutil"
	"log"
	"os"
)

func main() {

	infile := os.Args[1]
	outfile := os.Args[2]

	log.Println("Stripping", infile, "to", outfile)

	if contents, err := ioutil.ReadFile(infile); err != nil {
		log.Fatalln("Error:", e.Error)
	}
}
