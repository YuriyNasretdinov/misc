package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"crypto/md5"
	"math/rand"
	"time"

	"github.com/xwb1989/sqlparser"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s '<query>'", os.Args[0])
	}

	query := os.Args[1]

	query = strings.Replace(query, ".", "___", -1)

	rawRes, err := sqlparser.Parse(query)
	if err != nil {
		log.Fatalf("Could not parse sql: %s", err.Error())
	}

	res := rawRes.(*sqlparser.Select)

	buf := sqlparser.NewTrackedBuffer(nil)
	rewritten := make(map[string]string)
	orig := make(map[string]string)

	var i = 0

	rand.Seed(time.Now().UnixNano())

	var seed = make([]byte, 20)
	rand.Read(seed)

	Rewrite(res, func(what interface{}) []byte {
		switch buf := what.(type) {
		case []byte:
			str := strings.ToLower(string(buf))
			if sqlparser.Aggregates[str] {
				return buf
			}

			if res, ok := orig[string(buf)]; ok {
				return []byte(res)
			}

			i++
			identifier := fmt.Sprintf("ident%05d", i)
			// fmt.Fprintf(os.Stderr, "%s => %s\n", buf, identifier)
			rewritten[identifier] = string(buf)
			orig[string(buf)] = identifier
			return []byte(identifier)
		case sqlparser.StrVal:
			i++
			m := md5.New()
			m.Write(seed)
			m.Write([]byte(buf))

			blen := len(buf)

			if blen > 3 {
				s := fmt.Sprintf("%x", m.Sum(nil))[0:blen]
				//fmt.Fprintf(os.Stderr, "%s => %s\n", buf, s)
				rewritten[s] = string(buf)
				orig[string(buf)] = s
				return []byte(s)
			}

			return []byte(buf)
		case sqlparser.NumVal:
			i++
			m := rand.Int63()
			blen := len(buf)

			if blen > 5 {
				s := fmt.Sprintf("%d", m)[0:blen]
				//fmt.Fprintf(os.Stderr, "%s => %s\n", buf, s)
				rewritten[s] = string(buf)
				orig[string(buf)] = s
				return []byte(s)
			}

			return []byte(buf)
		default:
			log.Printf("Unrecognized type: %T", what)
			panic("See errors above")
		}
	})

	res.Format(buf)
	query = buf.String()
	fmt.Fprintf(os.Stderr, "Formatted: %s\n", query)

	resp, err := http.PostForm(
		"http://www.querymongo.com/",
		url.Values{"MySQLQuery": []string{query}},
	)

	if err != nil {
		log.Fatalf("Could not make request: %v", err.Error())
	}

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Could not read request: %v", err.Error())
	}

	parts := bytes.Split(contents, []byte(`<textarea id="mongoQuery" name="mongoQuery">`))
	if len(parts) == 1 {
		parts = bytes.Split(contents, []byte(`<div class="alert alert-error error">`))
		if len(parts) == 2 {
			parts = bytes.Split(parts[1], []byte(`</div>`))
			log.Fatal(strings.TrimSpace(string(parts[0])))
		}

		log.Fatal("Could not parse format, missing textarea")
	}

	parts = bytes.Split(parts[1], []byte(`</textarea>`))

	if len(parts) == 1 {
		log.Fatal("Could not parse format, missing closing tag for textarea")
	}

	result := parts[0]

	for old, new := range rewritten {
		result = bytes.Replace(result, []byte(old), []byte(new), -1)
	}

	result = bytes.Replace(result, []byte("___"), []byte("."), -1)

	fmt.Printf("%s\n", result)
}
