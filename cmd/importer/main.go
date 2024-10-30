package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	mssql "github.com/microsoft/go-mssqldb"
)

var filenames = []string{
	"airports.csv",
	"airport-frequencies.csv",
	"countries.csv",
	"navaids.csv",
	"regions.csv",
	"runways.csv",
}

const (
	base_url string = "https://davidmegginson.github.io/ourairports-data"

	airport_table_columns = `
		id int IDENTITY(1,1) CONSTRAINT pk_airports_id PRIMARY KEY,
		ident nvarchar(20),
		type nvarchar(20),
		name nvarchar(255),
		latitude_deg decimal(12, 9),
		longitude_deg decimal(12, 9),
		elevation_ft int,
		continent nvarchar(20),
		iso_country nvarchar(20),
		iso_region nvarchar(20),
		municipality nvarchar(255),
		scheduled_service nvarchar(20),
		gps_code nvarchar(20),
		iata_code nvarchar(20),
		local_code nvarchar(20),
		home_link nvarchar(512),
		wikipedia_link nvarchar(512),
		keywords nvarchar(512)
	`

	airport_frequency_table_columns = `
		id int IDENTITY(1,1) CONSTRAINT pk_airport_frequencies_id PRIMARY KEY,
		airport_ref int,
		airport_ident nvarchar(20),
		type nvarchar(20),
		description nvarchar(512),
		frequency_mhz nvarchar(20)
	`

	countries_table_columns = `
		id int IDENTITY(1,1) CONSTRAINT pk_countries_id PRIMARY KEY,
		code nvarchar(20),
		name nvarchar(255),
		continent nvarchar(20),
		wikipedia_link nvarchar(512),
		keywords nvarchar(512)
	`

	navaids_table_columns = `
		id int IDENTITY(1,1) CONSTRAINT pk_navaids_id PRIMARY KEY,
		filename nvarchar(255),
		ident nvarchar(20),
		name nvarchar(255),
		type nvarchar(20),
		frequency_khz int,
		latitude_deg decimal(12, 9),
		longitude_deg decimal(12, 9),
		elevation_ft int,
		iso_country nvarchar(20),
		dme_frequency_khz int,
		dme_channel nvarchar(20),
		dme_latitude_deg decimal(12, 9),
		dme_longitude_deg decimal(12, 9),
		dme_elevation_ft int,
		slaved_variation_deg decimal(12, 9),
		magnetic_variation_deg decimal(12, 9),
		usageType nvarchar(20),
		power nvarchar(20),
		associated_airport nvarchar(20)
	`

	regions_table_columns = `
		id int IDENTITY(1,1) CONSTRAINT pk_regions_id PRIMARY KEY,
		code nvarchar(20),
		local_code nvarchar(20),
		name nvarchar(255),
		continent nvarchar(20),
		iso_country nvarchar(20),
		wikipedia_link nvarchar(512),
		keywords nvarchar(512)
	`

	runways_table_columns = `
		id int IDENTITY(1,1) CONSTRAINT pk_runways_id PRIMARY KEY,
		airport_ref int,
		airport_ident nvarchar(20),
		length_ft int,
		width_ft int,
		surface nvarchar(255),
		lighted bit,
		closed bit,
		le_ident nvarchar(20),
		le_latitude_deg decimal(12, 9),
		le_longitude_deg decimal(12, 9),
		le_elevation_ft int,
		le_heading_degT int,
		le_displaced_threshold_ft int,
		he_ident nvarchar(20),
		he_latitude_deg decimal(12, 9),
		he_longitude_deg decimal(12, 9),
		he_elevation_ft int,
		he_heading_degT int,
		he_displaced_threshold_ft int
	`
)

func buildCreateTableStatement(table_name string) string {
	cols_with_types := airport_table_columns

	if table_name == "dbo.airport_frequencies" {
		cols_with_types = airport_frequency_table_columns
	} else if table_name == "dbo.countries" {
		cols_with_types = countries_table_columns
	} else if table_name == "dbo.navaids" {
		cols_with_types = navaids_table_columns
	} else if table_name == "dbo.regions" {
		cols_with_types = regions_table_columns
	} else if table_name == "dbo.runways" {
		cols_with_types = runways_table_columns
	}

	return fmt.Sprintf(`IF OBJECT_ID('%s', 'U') IS NULL CREATE TABLE %s(%s)`,
		table_name, table_name, cols_with_types)
}

func parseCSV(data []byte) (*csv.Reader, error) {
	reader := csv.NewReader(bytes.NewReader(data))

	return reader, nil
}

func dbConnectionString() string {
	server := os.Getenv("DB_SERVER")
	database := os.Getenv("DB_DATABASE")
	user_id := os.Getenv("DB_USERNAME")
	password := os.Getenv("DB_PASSWORD")
	port, _ := strconv.Atoi(os.Getenv("DB_PORT"))

	return fmt.Sprintf(
		"server=%s;database=%s;user id=%s;password=%s;port=%d",
		server, database, user_id, password, port,
	)
}

func processCSV(conn_str string, reader *csv.Reader, filename string) {
	db, err := sql.Open("mssql", conn_str)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	no_ext := strings.ReplaceAll(filename, ".csv", "")
	fn := strings.ReplaceAll(no_ext, "-", "_")
	table := fmt.Sprintf("dbo.%s", fn)

	// Drop and create table
	drop_stmt := fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s;", table, table)
	_, err = db.Exec(drop_stmt)
	if err != nil {
		log.Fatal(err)
	}

	// Read the header row
	record, _ := reader.Read()

	create_stmt := buildCreateTableStatement(table)

	_, err = db.Exec(create_stmt)
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(mssql.CopyIn(table, mssql.BulkOptions{}, record...))
	if err != nil {
		log.Fatal(err.Error())
	}

	for {
		record, err = reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error reading CSV data:", err)
			break
		}

		var c []any

		if table == "dbo.airports" {
			id, _ := strconv.Atoi(strings.TrimSpace(record[0]))
			lat, _ := strconv.ParseFloat(strings.TrimSpace(record[4]), 64)
			lon, _ := strconv.ParseFloat(strings.TrimSpace(record[5]), 64)
			elev, _ := strconv.Atoi(strings.TrimSpace(record[6]))

			c = append(c, id)
			c = append(c, strings.TrimSpace(record[1]))
			c = append(c, strings.TrimSpace(record[2]))
			c = append(c, strings.TrimSpace(record[3]))
			c = append(c, lat)
			c = append(c, lon)
			c = append(c, elev)
			c = append(c, strings.TrimSpace(record[7]))
			c = append(c, strings.TrimSpace(record[8]))
			c = append(c, strings.TrimSpace(record[9]))
			c = append(c, strings.TrimSpace(record[10]))
			c = append(c, strings.TrimSpace(record[11]))
			c = append(c, strings.TrimSpace(record[12]))
			c = append(c, strings.TrimSpace(record[13]))
			c = append(c, strings.TrimSpace(record[14]))
			c = append(c, strings.TrimSpace(record[15]))
			c = append(c, strings.TrimSpace(record[16]))
			c = append(c, strings.TrimSpace(record[17]))
		} else if table == "dbo.airport_frequencies" {
			id, _ := strconv.Atoi(strings.TrimSpace(record[0]))
			airport_ref, _ := strconv.Atoi(strings.TrimSpace(record[1]))

			c = append(c, id)
			c = append(c, airport_ref)
			c = append(c, strings.TrimSpace(record[2]))
			c = append(c, strings.TrimSpace(record[3]))
			c = append(c, strings.TrimSpace(record[4]))
			c = append(c, strings.TrimSpace(record[5]))
		} else if table == "dbo.countries" {
			id, _ := strconv.Atoi(strings.TrimSpace(record[0]))

			c = append(c, id)
			c = append(c, strings.TrimSpace(record[1]))
			c = append(c, strings.TrimSpace(record[2]))
			c = append(c, strings.TrimSpace(record[3]))
			c = append(c, strings.TrimSpace(record[4]))
			c = append(c, strings.TrimSpace(record[5]))
		} else if table == "dbo.navaids" {
			id, _ := strconv.Atoi(strings.TrimSpace(record[0]))
			frequency_khz, _ := strconv.Atoi(strings.TrimSpace(record[5]))
			lat, _ := strconv.ParseFloat(strings.TrimSpace(record[6]), 64)
			lon, _ := strconv.ParseFloat(strings.TrimSpace(record[7]), 64)
			elev, _ := strconv.Atoi(strings.TrimSpace(record[8]))
			dme_frequency_khz, _ := strconv.Atoi(strings.TrimSpace(record[10]))
			dme_latitude_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[12]), 64)
			dme_longitude_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[13]), 64)
			dme_elevation_ft, _ := strconv.Atoi(strings.TrimSpace(record[14]))
			slaved_variation_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[15]), 64)
			magnetic_variation_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[16]), 64)

			c = append(c, id) // id
			c = append(c, strings.TrimSpace(record[1]))
			c = append(c, strings.TrimSpace(record[2]))
			c = append(c, strings.TrimSpace(record[3]))
			c = append(c, strings.TrimSpace(record[4]))
			c = append(c, frequency_khz) // frequency_khz
			c = append(c, lat)           // lat
			c = append(c, lon)           // lon
			c = append(c, elev)          // elevation_ft
			c = append(c, strings.TrimSpace(record[9]))
			c = append(c, dme_frequency_khz)             // dme_frequency_khz
			c = append(c, strings.TrimSpace(record[11])) // dme_channel
			c = append(c, dme_latitude_deg)              // dme_latitude_deg
			c = append(c, dme_longitude_deg)             // dme_longitude_deg
			c = append(c, dme_elevation_ft)              // dme_elevation_ft
			c = append(c, slaved_variation_deg)          // slaved_variation_deg
			c = append(c, magnetic_variation_deg)        // magnetic_variation_deg
			c = append(c, strings.TrimSpace(record[17])) // usageType
			c = append(c, strings.TrimSpace(record[18])) // power
			c = append(c, strings.TrimSpace(record[19])) // associated_airport
		} else if table == "dbo.regions" {
			id, _ := strconv.Atoi(strings.TrimSpace(record[0]))

			c = append(c, id)                           // id
			c = append(c, strings.TrimSpace(record[1])) // code
			c = append(c, strings.TrimSpace(record[2])) // local_code
			c = append(c, strings.TrimSpace(record[3])) // name
			c = append(c, strings.TrimSpace(record[4])) // continent
			c = append(c, strings.TrimSpace(record[5])) // iso_country
			c = append(c, strings.TrimSpace(record[6])) // wikipedia_link
			c = append(c, strings.TrimSpace(record[7])) // keywords
		} else if table == "dbo.runways" {
			id, _ := strconv.Atoi(strings.TrimSpace(record[0]))
			airport_ref, _ := strconv.Atoi(strings.TrimSpace(record[1]))
			length, _ := strconv.Atoi(strings.TrimSpace(record[3]))
			width, _ := strconv.Atoi(strings.TrimSpace(record[4]))
			lighted, _ := strconv.ParseBool(strings.TrimSpace(record[6]))
			closed, _ := strconv.ParseBool(strings.TrimSpace(record[7]))
			le_latitude_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[9]), 64)
			le_longitude_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[10]), 64)
			le_elevation_ft, _ := strconv.Atoi(strings.TrimSpace(record[11]))
			le_heading_degT, _ := strconv.Atoi(strings.TrimSpace(record[12]))
			le_displaced_threshold_ft, _ := strconv.Atoi(strings.TrimSpace(record[13]))
			he_latitude_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[9]), 64)
			he_longitude_deg, _ := strconv.ParseFloat(strings.TrimSpace(record[10]), 64)
			he_elevation_ft, _ := strconv.Atoi(strings.TrimSpace(record[11]))
			he_heading_degT, _ := strconv.Atoi(strings.TrimSpace(record[12]))
			he_displaced_threshold_ft, _ := strconv.Atoi(strings.TrimSpace(record[13]))

			c = append(c, id)                            // id
			c = append(c, airport_ref)                   // airport_ref
			c = append(c, strings.TrimSpace(record[2]))  // airport_ident
			c = append(c, length)                        // length
			c = append(c, width)                         // width
			c = append(c, strings.TrimSpace(record[5]))  // surface
			c = append(c, lighted)                       // lighted
			c = append(c, closed)                        // closed
			c = append(c, strings.TrimSpace(record[8]))  // le_ident
			c = append(c, le_latitude_deg)               // le_latitude_deg
			c = append(c, le_longitude_deg)              // le_longitude_deg
			c = append(c, le_elevation_ft)               // le_elevation_ft
			c = append(c, le_heading_degT)               // le_heading_degT
			c = append(c, le_displaced_threshold_ft)     // le_displaced_threshold_ft
			c = append(c, strings.TrimSpace(record[14])) // he_ident
			c = append(c, he_latitude_deg)               // he_latitude_deg
			c = append(c, he_longitude_deg)              // he_longitude_deg
			c = append(c, he_elevation_ft)               // he_elevation_ft
			c = append(c, he_heading_degT)               // he_heading_degT
			c = append(c, he_displaced_threshold_ft)     // he_displaced_threshold_ft
		}

		_, err = stmt.Exec(c...)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	rowCount, _ := result.RowsAffected()
	log.Printf("%d row(s) copied\n", rowCount)
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	conn_str := dbConnectionString()

	for _, filename := range filenames {
		url := fmt.Sprintf("%s/%s", base_url, filename)
		log.Printf("Downloading: %s", url)

		resp, err := http.Get(url)
		if err != nil {
			log.Fatal(err)
		}

		defer resp.Body.Close()

		data, err := io.ReadAll(resp.Body)

		if err != nil {
			fmt.Println("Error reading file:", err)

			return
		}

		log.Println("File downloaded, parsing CSV now...")

		reader, err := parseCSV(data)
		if err != nil {
			fmt.Println("Error creating CSV reader:", err)

			return
		}

		processCSV(conn_str, reader, filename)
	}
}
