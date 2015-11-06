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

	contents, err := ioutil.ReadFile(infile)
	if err != nil {
		log.Fatalln("Error:", err.Error)
	}
	log.Println("Read in ", len(contents), "bytes from", infile)

	inquotes := false
	linenumber := 1
	killed := 0
	outputbytes := make([]byte, 0, len(contents))
	//var output []byte

	// Apply the stripper algorithm to the input bytes
	for _, b := range contents {
		if b == '"' {
			inquotes = !inquotes
		}
		switch b {
		case '\r':
			if inquotes {
				log.Println("CR in quotes on line", linenumber)
				killed++
			}
			break
		case '\n':
			if inquotes {
				log.Println("LF in quotes on line", linenumber)
				killed++
			} else {
				log.Println("New line", linenumber)
				outputbytes = append(outputbytes, b)
			}
			linenumber++
			break
		default:
			outputbytes = append(outputbytes, b)
			break
		}
	}

	// Now write the output bytes to the output file
	err = ioutil.WriteFile(outfile, outputbytes, 0777)
	if err != nil {
		log.Println("WriteFile :", err.Error())
	}
	log.Println("Ended up killing", killed, "bytes")
}
