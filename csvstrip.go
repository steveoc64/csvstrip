package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"database/sql"
	_ "github.com/lib/pq"
)

var debug bool
var ignoreFld int
var field int
var linenumber int
var charoffset int
var DSN string
var ignoredData []byte

func addTo(s []byte, b byte) []byte {
	if field == ignoreFld {
		//log.Println("Ignore", linenumber, charoffset, field, b)
		ignoredData = append(ignoredData, b)		
		return s
	}
	return append(s, b)
}

func main() {

	// Load runtime flags
	var debugPtr = flag.Bool("debug", false, "Show debug messages")
	var ignoreFieldPtr = flag.Int("ignore", -1, "Ignore Field")
	flag.StringVar(&DSN, "dsn", "", "Data Source Name")

	flag.Parse()
	debug = *debugPtr
	ignoreFld = *ignoreFieldPtr

	log.Println("Non flag args", flag.Args())
	infile := flag.Arg(0)
	outfile := flag.Arg(1)

	if debug {
		log.Println("Stripping", infile, "to", outfile)
		if ignoreFld != -1 {
			log.Println("Ignoring field", ignoreFld)
		}
	}

	// Connect to the database
	var stmt *sql.Stmt

	if DSN != "" {
		if debug {
			log.Println("Connecting to database", DSN)
		}
		db, err := sql.Open("postgres", DSN)
		defer db.Close()
		if err != nil {
			log.Fatalln("Exiting ..", err.Error())
		}
		if debug {
			log.Println("Connected to database ...", DSN)
		}
		stmt, err = db.Prepare("update fm_task set instructions = ($1) where lineno = $2")
		if err != nil {
			log.Fatalln("Prepare Statement:", err.Error())
		}

	}

	contents, err := ioutil.ReadFile(infile)
	if err != nil {
		log.Fatalln("Error:", err.Error)
	}
	if debug {
		log.Println("Read in ", len(contents), "bytes from", infile)
	}

	inquotes := false
	linenumber = 1
	charoffset = 0
	field = 1
	killed := 0
	added := 0
	outputbytes := make([]byte, 0, len(contents))
	ignoredData = make([]byte, 0, 10000)

	// Apply the stripper algorithm to the input bytes
	for index, b := range contents {
		charoffset++
		switch b {
		//case '\r':
		//	log.Println("Stripping CR from line", linenumber)
		//	killed++
		//	break
		case '"':
			// If we are not in quotes, then treat it as OK
			// If we are in quotes then it may be the end of the quoted string
			//  ... if the next char is a comma (,) or end of line (\r or \n), then treat it as OK
			// Otherwise - it is a quote inside a field, so we need to escape it out
			//			log.Println("Quote char at", linenumber, charoffset, inquotes)
			if inquotes {
				if index+1 >= len(contents) {
					//log.Println("Found Quote inside a quote we are at the end of the file - all good !", linenumber, charoffset)
					outputbytes = append(outputbytes, b)
					inquotes = false
				} else {
					switch contents[index+1] {
					case '\r', '\n':
						//	log.Println("Found Quote inside a quote and next char is a newline, so treat it as the end of the quote", linenumber, charoffset)
						outputbytes = append(outputbytes, b)
						inquotes = false
					case ',':
						//	log.Println("Found quote inside a quote followed by comma, so treat this as the end of the field", linenumber, charoffset)
						outputbytes = append(outputbytes, b)
						inquotes = false
					default:
						if debug {
							log.Println("Found quote inside a quote, and the next char is", contents[index+1], "so treat this as an embedded quote", linenumber, charoffset)
						}
						outputbytes = addTo(outputbytes, '\\')
						outputbytes = addTo(outputbytes, b)
						added++
					}
				}
			} else {
				outputbytes = append(outputbytes, b)
				inquotes = true
			}
			break
		case '\r':
			if inquotes {
				if debug {
					log.Println("CR in quotes on line, so add Escape char", linenumber, charoffset, field)
				}
				outputbytes = addTo(outputbytes, '\\')
				outputbytes = addTo(outputbytes, 'r')
				added++
			} else {
				if field == ignoreFld {
					// Special hack here, add an extra column to the data with the line number of the line being processed
					// Note that we use linenumber-1 since the first line is a header !!!
					if debug {
						log.Println("Appending an extra field for linenumber", linenumber-1)
						outputbytes = append(outputbytes, fmt.Sprintf(",\"%d\"", linenumber-1)...)
					}
				}
				outputbytes = append(outputbytes, b)

				// Now we should update the database with the contents of the missing field
				if debug {
					if DSN != "" && stmt != nil {
						//res, err := stmt.Exec(fmt.Sprintf("instructions %d", linenumber), linenumber)
						log.Println("\n\nSetting Instructions As:\n=============================\n\n",string(ignoredData),"\n=========================\n")
						res, err := stmt.Exec(string(ignoredData), linenumber)
						if err != nil {
							log.Println("ERROR:", err.Error())
						} else if debug {
							log.Println("Updated", res, "rows")
						}
					}
				}
			}
			break
		case '\n':
			if inquotes {
				if debug {
					log.Println("LF in quotes on line, so add Escape char", linenumber, charoffset, field)
				}
				outputbytes = addTo(outputbytes, '\\')
				outputbytes = addTo(outputbytes, 'n')
				added++
			} else {
				outputbytes = append(outputbytes, b)
			}
			linenumber++
			charoffset = 1
			field = 1
			if debug {
				log.Println("New line", linenumber)
			}
			ignoredData = make([]byte, 0, 10000)
			//log.Println("Previous 2 chars are ", contents[index-2], contents[index-1])
			break
		case ',':
			if inquotes {
				if debug {
					log.Println("Found a comma inside a quote, so escape that out", linenumber, charoffset, field)
				}
				outputbytes = addTo(outputbytes, '\\')
				outputbytes = addTo(outputbytes, b)
				added++
			} else {
				outputbytes = append(outputbytes, b)
				field++
				if debug {
					log.Println("We are now in field", field)
				}
				if field == ignoreFld {
					if debug {
						log.Println("Ignoring Field", field)
					}
				}
			}
			break
		case '\\':
			if debug {
				log.Println("Escaping out a BackSlash", linenumber, charoffset, field)
			}
			outputbytes = addTo(outputbytes, '\\')
			outputbytes = addTo(outputbytes, 'b')
			added++
			break
		case '\v':
			if debug {
				log.Println("Escaping out a Vertical Tab", linenumber, charoffset, field)
			}
			outputbytes = addTo(outputbytes, '\\')
			outputbytes = addTo(outputbytes, 'v')
			added++
			break
		case '\t':
			if debug {
				log.Println("Escaping out a Tab", linenumber, charoffset, field)
			}
			outputbytes = addTo(outputbytes, '\\')
			outputbytes = addTo(outputbytes, 't')
			added++
			break
		case '\f':
			if debug {
				log.Println("Escaping out a FormFeed", linenumber, charoffset, field)
			}
			outputbytes = addTo(outputbytes, '\\')
			outputbytes = addTo(outputbytes, 'f')
			added++
			break
		default:
			outputbytes = addTo(outputbytes, b)
			break
		}
	}

	// Now write the output bytes to the output file
	err = ioutil.WriteFile(outfile, outputbytes, 0777)
	if err != nil {
		log.Println("WriteFile :", err.Error())
	}
	if debug {
		log.Println("Ended up killing", killed, "bytes, and adding", added)
	}
}
