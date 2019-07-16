package mediumstats

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/functions/metadata"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
)

// projectID is set from the GCP_PROJECT environment variable, which is
// automatically set by the Cloud Functions runtime.
var projectID = os.Getenv("GCP_PROJECT")
var instance = os.Getenv("SPANNER_INSTANCE")
var database = os.Getenv("SPANNER_DATABASE")

// client is a global Pub/Sub client, initialized once per instance.
var spannerclient *spanner.Client
var storageclient *storage.Client

func init() {
	// err is pre-declared to avoid shadowing client.
	var err error

	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		projectID,
		instance,
		database)

	// client is initialized with context.Background() because it should
	// persist between function invocations.
	spannerclient, err = spanner.NewClient(context.Background(), dsn)
	if err != nil {
		log.Fatalf("spanner.NewClient: %v", err)
	}

	storageclient, err = storage.NewClient(context.Background())
	if err != nil {
		log.Fatalf("storage.NewClient: %v", err)
	}
}

// GCSEvent is the payload of a GCS event.
type GCSEvent struct {
	Bucket         string    `json:"bucket"`
	Name           string    `json:"name"`
	Metageneration string    `json:"metageneration"`
	ResourceState  string    `json:"resourceState"`
	TimeCreated    time.Time `json:"timeCreated"`
	Updated        time.Time `json:"updated"`
}

// GCStoSpanner import medium stats (CSV) to Cloud Spanner.
func GCStoSpanner(ctx context.Context, e GCSEvent) error {
	meta, err := metadata.FromContext(ctx)
	if err != nil {
		return fmt.Errorf("metadata.FromContext: %v", err)
	}
	log.Printf("Event ID: %v\n", meta.EventID)
	log.Printf("Event type: %v\n", meta.EventType)
	log.Printf("Bucket: %v\n", e.Bucket)
	log.Printf("File: %v\n", e.Name)
	log.Printf("Metageneration: %v\n", e.Metageneration)
	log.Printf("Created: %v\n", e.TimeCreated)
	log.Printf("Updated: %v\n", e.Updated)

	// Get the bucket name and create a handler for the file
	bucketName := e.Bucket
	rc, err := storageclient.Bucket(bucketName).Object(e.Name).NewReader(ctx)
	if err != nil {
		return err
	}
	defer rc.Close()

	// Extract the file creation name from thte file name using regexp
	re := regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
	fileCreationDates := re.FindAllString(e.Name, -1)
	fileCreationDate := fileCreationDates[0]

	// Read the content of the file
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return err
	}

	// Initialize the Cloud Spanner mutations array
	m := []*spanner.Mutation{}

	// Loop through each line of the file and write the data to Cloud Spanner
	lines := strings.Split(string(data), "\n")
	log.Printf("Raw data :%s\n", lines)
	log.Printf("Number of articles: %d\n", len(lines)-1)

	// Start from i=1 to skip the header line 0
	for i := 1; i < len(lines); i++ {
		line := strings.Split(lines[i], "|")
		if len(line) == 11 {
			log.Printf("Aricle %s has %d columns\n", line[0], len(line))
			articleColumns := []string{
				"id", "title", "link", "publication", "mins", "views",
				"reads", "readRatio", "fans", "pubDate", "liveDate",
				"lastUpdateTime",
			}
			// Remove the string " min read"
			mins := strings.Replace(line[4], " min read", "", -1)
			// String to FLOAT64 then INT64 conversion
			readRatio, _ := strconv.ParseFloat(line[7], 64)

			currentDay := time.Now().Local().Format("2006-01-02")
			// Make sure that we don't overwrite with old data
			if currentDay == fileCreationDate {
				sm := spanner.InsertOrUpdate("stats", articleColumns, []interface{}{
					line[0], line[1], line[2], line[3], //strings
					mins, line[5], line[6], //integers
					readRatio,         //float
					line[8],           //integer
					line[9], line[10], //dates
					spanner.CommitTimestamp,
				})
				m = append(m, sm)
			} else {log.Printf("Skipping article stats update because ingested file is old")}

			readviewColumns := []string{
				"id", "updateTime", "views", "reads",
			}
			rm := spanner.InsertOrUpdate("readview_history", readviewColumns, []interface{}{
				line[0], fileCreationDate, line[5], line[6],
			})
			m = append(m, rm)
		}
	}

	_, err = spannerclient.Apply(ctx, m)
	if err != nil {
		log.Println(err)
		return err
	}

	return err
}
