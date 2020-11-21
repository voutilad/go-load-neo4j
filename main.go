package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

const BATCH_SIZE = 5000
const MAX_CONCURRENCY = 12

const CYPHER = `
UNWIND $batch AS item
CREATE (n:Node {type: item.type, id: item.id})
SET n += item.properties
`

// I have some sample JSON data with a few properties
type Properties struct {
	Id string `json:"id"`
}
type Record struct {
	Type       string     `json:"type"`
	Id         string     `json:"id"`
	Label      string     `json:"label"`
	Labels     []string   `json:"labels"`
	Properties Properties `json:"properties"`
}

// Convert the Record type to a Neo4j-friendly parameter type
func (r Record) toNeo4jParam() map[string]interface{} {
	return map[string]interface{}{
		"type":   r.Type,
		"id":     r.Id,
		"labels": r.Labels,
		"properties": map[string]interface{}{
			"id": r.Properties.Id,
		},
	}
}

// Parse JSON Objects out from our input stream
func decode(r io.Reader, out chan<- Record) {
	decoder := json.NewDecoder(os.Stdin)

	for {
		var r Record
		if err := decoder.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		out <- r
	}
	close(out)
}

// Where the Magic Happens!
func doWork(batch []map[string]interface{}, driver *neo4j.Driver, db string) {
	session := (*driver).NewSession(neo4j.SessionConfig{
		DatabaseName: db,
	})
	defer session.Close()

	params := map[string]interface{}{
		"batch": batch,
	}

	_, err := session.WriteTransaction(
		func(tx neo4j.Transaction) (interface{}, error) {
			res, err := tx.Run(CYPHER, params)
			if err != nil {
				log.Fatal(err)
			}
			summary, err := res.Consume()
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("created %d nodes from batch\n", summary.Counters().NodesCreated())
			return true, nil
		})

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// Wire up a stream reader
	records := make(chan Record, 5)
	go decode(os.Stdin, records)

	// get config from environment for now
	dbUri, found := os.LookupEnv("NEO4J_URI")
	if !found {
		dbUri = "bolt://localhost:7687"
	}
	password, found := os.LookupEnv("NEO4J_PASSWORD")
	if !found {
		password = "password"
	}
	user, found := os.LookupEnv("NEO4J_USER")
	if !found {
		user = "neo4j"
	}
	dbName, found := os.LookupEnv("NEO4J_DB")
	if !found {
		dbName = "neo4j"
	}
	var err error
	var maxConcurrency int
	maxConcurrencyStr, found := os.LookupEnv("NEO4J_MAX_WORKERS")
	if !found {
		maxConcurrency = MAX_CONCURRENCY
	} else {
		maxConcurrency, err = strconv.Atoi(maxConcurrencyStr)
		if err != nil {
			log.Fatal(err)
		}
	}
	var batchSize int
	batchSizeStr, found := os.LookupEnv("NEO4J_BATCH_SIZE")
	if !found {
		batchSize = BATCH_SIZE
	} else {
		batchSize, err = strconv.Atoi(batchSizeStr)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Try initializing our driver and check connectivity
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth(user, password, ""),
		func(config *neo4j.Config) {
			config.UserAgent = "go-loader/v0"
		})
	if err != nil {
		log.Fatal(err)
	}
	if err := driver.VerifyConnectivity(); err != nil {
		log.Fatal(err)
	}

	// Set up some helper data structures
	batch := make([]map[string]interface{}, 0, batchSize)
	availableWorkers := make(chan bool, maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		availableWorkers <- true
	}
	var wg sync.WaitGroup

	// Work over incoming records, building batches
	cnt := 0
	for record := range records {
		batch = append(batch, record.toNeo4jParam())
		cnt++

		if len(batch) >= batchSize {

			// I don't understand Go enough...but I found I need to copy() here
			data := make([]map[string]interface{}, batchSize)
			copy(data, batch)

			log.Printf("batching %d records...\n", len(data))
			select {
			case <-availableWorkers:
				go func() {
					// increment workgroup
					wg.Add(1)
					// actual do something productive
					doWork(data, &driver, dbName)
					// decrement workgroup, signal resource available
					wg.Done()
					availableWorkers <- true
				}()
			case <-time.After(120 * time.Second):
				log.Fatal("timed out after 120 seconds...aborting!")
			}

			// Allocate new memory for another batch
			batch = make([]map[string]interface{}, 0, batchSize)
		}
	}

	// check for leftovers
	if len(batch) > 0 {
		// be lazy...just spin up a worker. should check for max concurrency
		go func() {
			wg.Add(1)
			doWork(batch, &driver, dbName)
			wg.Done()
		}()
	}

	// Wait for any remaining workers
	wg.Wait()
	log.Printf("processed %d records\n", cnt)
}
