package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

//defines what data is in the csv file
type apidata struct {
	Method string    `csv:"request_method"`
	URL    string    `csv:"url"`
	Date   time.Time `csv:"date_accessed"`
	Bytes  int       `csv:"bytes_streamed"`
}

//reads csv file into a struct we can work with
func readcsv(csvfile string) ([]*apidata, error) {

	data := []*apidata{}

	f, err := os.Open(csvfile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := gocsv.UnmarshalFile(f, &data); err != nil {
		return nil, err
	}
	return data, nil
}

//takes apidata and gets the resource that had the most bytes streamed within a month
//returning that resource+month combo and total bytes streamed
func getbytesstreamed(data []*apidata) (map[string]int, error) {
	sermons := make(map[string]int)
	for _, item := range data {
		//split date on -
		splitdate := strings.Split(item.Date.String(), "-")
		//grab the chunk of date we care about
		fullname := item.URL + "-" + splitdate[0] + "-" + splitdate[1]
		//check to see if the sermon already exists in the map
		value, ok := sermons[fullname]
		//if it does, add more bytes to the total
		if ok {
			newbytes := value + item.Bytes
			sermons[fullname] = newbytes
		} else {
			sermons[fullname] = value
		}
	}
	return sermons, nil
}

func getMostStreamed(sermons map[string]int) string {
	// used to switch key and value
	sdata := map[int]string{}
	sbytes := []int{}
	for key, val := range sermons {
		sdata[val] = key
		sbytes = append(sbytes, val)
	}
	var mostStreamed []string
	// sortedData := map[string]int{}
	for _, val := range sbytes {
		streamitem := fmt.Sprintf("%s: with %v bytes transferred.\n", sdata[val], val)
		mostStreamed = append(mostStreamed, streamitem)
	}
	//return the top item
	return mostStreamed[0]
}

func main() {
	//read csv file
	apidata, err := readcsv("test.csv")
	if err != nil {
		log.Fatalf("Unable to read csv file, %s", err)
	}
	//get a map of sermons and bytes
	sermons, err := getbytesstreamed(apidata)
	if err != nil {
		log.Fatalf("Unable to parse csv file, %s", err)
	}
	moststreamed := getMostStreamed(sermons)
	fmt.Printf("The resource that had the most bytes streamed %s", moststreamed)
}
