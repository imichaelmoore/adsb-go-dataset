package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

var (
	BATCH_SIZE              int
	DATASET_API_WRITE_TOKEN string
	DUMP1090_HOST           string
	DUMP1090_PORT           string
	COLLECTOR_SOURCE        string
)

// Initialize configuration using command-line arguments or environment variables
func initializeConfiguration() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dataset_api_write_token",
				Usage:       "Set the dataset_api_write_token for authentication. You can also set this via the DATASET_API_WRITE_TOKEN environment variable.",
				EnvVars:     []string{"DATASET_API_WRITE_TOKEN"},
				Destination: &DATASET_API_WRITE_TOKEN,
			},
			&cli.StringFlag{
				Name:        "dump1090_host",
				Usage:       "Set the DUMP1090 host. You can also set this via the DUMP1090_HOST environment variable.",
				EnvVars:     []string{"DUMP1090_HOST"},
				Destination: &DUMP1090_HOST,
			},
			&cli.StringFlag{
				Name:        "dump1090_port",
				Value:       "30003",
				Usage:       "Set the DUMP1090 port. You can also set this via the DUMP1090_PORT environment variable.",
				EnvVars:     []string{"DUMP1090_PORT"},
				Destination: &DUMP1090_PORT,
			},
			&cli.IntFlag{
				Name:        "batch_size",
				Value:       500,
				Usage:       "Set the batch size for processing. Defaults to 500. You can also set this via the BATCH_SIZE environment variable.",
				EnvVars:     []string{"BATCH_SIZE"},
				Destination: &BATCH_SIZE,
			},
			&cli.StringFlag{
				Name:        "collector_source",
				Value:       "dump1090",
				Usage:       "Set the collector source. Defaults to 'dump1090'. You can also set this via the COLLECTOR_SOURCE environment variable.",
				EnvVars:     []string{"COLLECTOR_SOURCE"},
				Destination: &COLLECTOR_SOURCE,
			},
		},
		Action: func(c *cli.Context) error {
			if DATASET_API_WRITE_TOKEN == "" {
				return fmt.Errorf("dataset_api_write_token is not set. Please provide it as a command-line argument or set the DATASET_API_WRITE_TOKEN environment variable. Example: --dataset_api_write_token=YOUR_TOKEN or export DATASET_API_WRITE_TOKEN=YOUR_TOKEN")
			}
			if DUMP1090_HOST == "" {
				return fmt.Errorf("dump1090_host is not set. Please provide it as a command-line argument or set the DUMP1090_HOST environment variable. Example: --dump1090_host=YOUR_HOST or export DUMP1090_HOST=YOUR_HOST")
			}
			return runApp()
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// SBS1Message represents the data structure for ADS-B messages.
type SBS1Message struct {
	Timestamp        string     `json:"timestamp"`
	MessageType      string     `json:"message_type,omitempty"`
	TransmissionType int32      `json:"transmission_type,omitempty"`
	SessionID        string     `json:"session_id,omitempty"`
	AircraftID       string     `json:"aircraft_id,omitempty"`
	Icao24           string     `json:"icao24,omitempty"`
	FlightID         string     `json:"flight_id,omitempty"`
	GeneratedDate    *time.Time `json:"generated_date,omitempty"`
	LoggedDate       *time.Time `json:"logged_date,omitempty"`
	Callsign         string     `json:"callsign,omitempty"`
	Altitude         int32      `json:"altitude,omitempty"`
	GroundSpeed      float32    `json:"ground_speed,omitempty"`
	Track            float32    `json:"track,omitempty"`
	Lat              float32    `json:"lat,omitempty"`
	Lon              float32    `json:"lon,omitempty"`
	VerticalRate     int32      `json:"vertical_rate,omitempty"`
	Squawk           int32      `json:"squawk,omitempty"`
	Alert            bool       `json:"alert,omitempty"`
	Emergency        bool       `json:"emergency,omitempty"`
	Spi              bool       `json:"spi,omitempty"`
	OnGround         bool       `json:"on_ground,omitempty"`
}

// NewSBS1Message initializes a new SBS1Message with the current timestamp.
func NewSBS1Message() SBS1Message {
	timestamp := time.Now().UnixNano()
	return SBS1Message{
		Timestamp: strconv.FormatInt(timestamp, 10),
	}
}

// Parse processes a raw message string and converts it into an SBS1Message.
func Parse(msg string) (SBS1Message, bool) {
	// log.Println("Parsing message:", msg)

	sbs1 := NewSBS1Message()
	parts := strings.Split(strings.TrimSpace(msg), ",")

	if len(parts) < 22 || parts[0] != "MSG" {
		return sbs1, false
	}

	sbs1.MessageType = "MSG"
	sbs1.TransmissionType = parseInt(parts[1])
	sbs1.SessionID = parts[2]
	sbs1.AircraftID = parts[3]
	sbs1.Icao24 = parts[4]
	sbs1.FlightID = parts[5]
	sbs1.GeneratedDate = parseDateTime(parts[6], parts[7])
	sbs1.LoggedDate = parseDateTime(parts[8], parts[9])
	sbs1.Callsign = strings.TrimSpace(parts[10])
	sbs1.Altitude = parseInt(parts[11])
	sbs1.GroundSpeed = parseFloat(parts[12])
	sbs1.Track = parseFloat(parts[13])
	sbs1.Lat = parseFloat(parts[14])
	sbs1.Lon = parseFloat(parts[15])
	sbs1.VerticalRate = parseInt(parts[16])
	sbs1.Squawk = parseInt(parts[17])
	sbs1.Alert = parseBool(parts[18])
	sbs1.Emergency = parseBool(parts[19])
	sbs1.Spi = parseBool(parts[20])
	sbs1.OnGround = parseBool(parts[21])

	return sbs1, true
}

// parseInt converts a string to int32, returning 0 on failure.
func parseInt(s string) int32 {
	if i, err := strconv.Atoi(s); err == nil {
		return int32(i)
	}
	return 0
}

// parseFloat converts a string to float32, returning 0.0 on failure.
func parseFloat(s string) float32 {
	if f, err := strconv.ParseFloat(s, 32); err == nil {
		return float32(f)
	}
	return 0
}

// parseBool converts a string to a bool, returning false on failure.
func parseBool(s string) bool {
	if b, err := strconv.Atoi(s); err == nil {
		return b != 0
	}
	return false
}

// parseDateTime converts date and time strings into a time.Time pointer.
func parseDateTime(date, timeStr string) *time.Time {
	layout := "2006/01/02 15:04:05"
	dt, err := time.Parse(layout, fmt.Sprintf("%s %s", date, timeStr))
	if err != nil {
		return nil
	}
	return &dt
}

// sendToService sends a batch of SBS1Messages to the Scalyr service.
func sendToService(messages []SBS1Message) error {
	log.Printf("Sending %d messages to the service", len(messages))

	events := make([]map[string]interface{}, len(messages))
	for i, message := range messages {
		events[i] = map[string]interface{}{
			"parser": "adsb",
			"ts":     message.Timestamp,
			"sev":    3,
			"attrs": map[string]interface{}{
				"message":   message,
				"source":    "dump1090-fa",
				"collector": "imichaelmoore/adsb-go-dataset",
				"parser":    "adsb",
			},
		}
	}

	payload := map[string]interface{}{
		"session":     uuid.New(),
		"sessionInfo": map[string]string{},
		"events":      events,
		"threads":     []string{},
	}

	client := &http.Client{}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "https://app.scalyr.com/api/addEvents", strings.NewReader(string(data)))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+DATASET_API_WRITE_TOKEN)

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, _ := io.ReadAll(res.Body)
	log.Printf("Response: %s", body)
	return nil
}

func main() {
	initializeConfiguration()
}

// runApp is the core functionality once configuration is set
func runApp() error {
	log.Println("Starting application...")

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", DUMP1090_HOST, DUMP1090_PORT))
	if err != nil {
		log.Fatal("Error connecting to DUMP1090:", err)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	messages := make([]SBS1Message, 0, BATCH_SIZE)

	for scanner.Scan() {
		msg := scanner.Text()
		if parsed, ok := Parse(msg); ok {
			messages = append(messages, parsed)
			if len(messages) >= BATCH_SIZE {
				err := sendToService(messages)
				if err != nil {
					log.Println("Error sending messages:", err)
				}
				messages = messages[:0] // Clear the slice
			}
		}
	}

	if len(messages) > 0 {
		err := sendToService(messages)
		if err != nil {
			log.Println("Error sending remaining messages:", err)
		}
	}

	log.Println("Exiting application...")
	return nil
}
