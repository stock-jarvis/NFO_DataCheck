package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	input_path := os.Args[1]
	CheckErr(input_path)
}

func CheckErr(inPath string) {
	var wg sync.WaitGroup
	input_path := inPath
	paths := []string{}
	filepath.Walk(input_path, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(info.Name(), ".csv") {
			paths = append(paths, path)
		}
		return nil
	})
	ta := 0
	//tc := 0
	//log.Println(len(paths))
	error_in_date := []string{}
	error_in_ts := []string{}
	error_in_hours := []string{}
	error_in_mins := []string{}
	error_in_LTP := []string{}
	error_in_BP := []string{}
	error_in_BQ := []string{}
	error_in_SP := []string{}
	error_in_SQ := []string{}
	error_in_LTQ := []string{}
	error_in_OI := []string{}
	for _, filepath := range paths {
		file, err := os.Open(filepath)
		//log.Println("file opened")
		if err != nil {
			log.Print("Cannot open file")
			wg.Done()
			return
		}
		wg.Add(1)
		ta++
		//log.Printf("Thread added %d", ta)
		go func(filepath string) {
			//log.Println("entered go func")
			csvReader := csv.NewReader(file)
			contents, _ := csvReader.ReadAll()
			f := fmt.Sprint(filepath)
			for j := 1; j < len(contents); j++ {
				for i := 1; i < len(contents[j]); i++ {
					switch i {
					case 1:
						_, err := time.Parse("02/01/2006", contents[j][1])
						if err != nil {
							error_in_date = append(error_in_date, fmt.Sprintf("\nThread:%d , %v , Error in date %v, Row: %d  ", ta, f, contents[j][1], j+1))
						}

					case 2:
						ts, err := time.Parse("15:04:05", contents[j][2])
						if err != nil {
							error_in_ts = append(error_in_ts, fmt.Sprintf("\nThread:%d ,%v , Error in timestamp %v, Row: %d  ", ta, f, contents[j][2], j+1))

						}
						h, m, _ := ts.Clock()
						if h < 9 || h > 15 {
							error_in_hours = append(error_in_hours, fmt.Sprintf("\nThread:%d ,%v , Error in hours %v, Row: %d  ", ta, f, contents[j][2], j+1))
						}
						if h == 9 && m < 15 {
							error_in_mins = append(error_in_mins, fmt.Sprintf("\nThread:%d ,%v , Hour = 9, Minutes < 15 %v, Row: %d  ", ta, f, contents[j][2], j+1))
						}
						if h == 15 && m > 30 {
							error_in_mins = append(error_in_mins, fmt.Sprintf("\nThread:%d ,%v , Hour = 15, Minutes > 29 %v, Row: %d  ", ta, f, contents[j][2], j+1))

						}
					case 3:
						_, err := strconv.ParseFloat(contents[j][3], 64)
						if err != nil {
							error_in_LTQ = append(error_in_LTQ, fmt.Sprintf("\nThread:%d ,%v , Error in LTP  %v, Row: %d  ", ta, f, contents[j][3], j+1))
						}

					case 4:
						_, err := strconv.ParseFloat(contents[j][4], 64)
						if err != nil {
							error_in_BP = append(error_in_BP, fmt.Sprintf("\nThread:%d ,%v , Buy Price %v, Row: %d  ", ta, f, contents[j][4], j+1))
						}

					case 5:
						_, err := strconv.ParseInt(contents[j][5], 36, 64)
						if err != nil {
							error_in_BQ = append(error_in_BQ, fmt.Sprintf("\nThread:%d ,%v , Error in Buy Quantity %v, Row: %d  ", ta, f, contents[j][5], j+1))
						}
					case 6:
						_, err := strconv.ParseFloat(contents[j][6], 64)
						if err != nil {
							error_in_SP = append(error_in_SP, fmt.Sprintf("\nThread:%d ,%v , Error in Sell Price %v, Row: %d  ", ta, f, contents[j][6], j+1))

						}

					case 7:
						_, err := strconv.ParseInt(contents[j][7], 36, 64)
						if err != nil {
							error_in_SQ = append(error_in_SQ, fmt.Sprintf("\nThread:%d ,%v , Error in Sell Quantity %v, Row: %d  ", ta, f, contents[j][7], j+1))

						}

					case 8:
						_, err := strconv.ParseInt(contents[j][8], 36, 64)
						if err != nil {
							error_in_LTQ = append(error_in_LTQ, fmt.Sprintf("\nThread:%d ,%v , Error in LTQ %v, Row: %d  ", ta, f, contents[j][8], j+1))
						}

					case 9:
						_, err := strconv.ParseInt(contents[j][8], 36, 64)
						if err != nil {
							error_in_OI = append(error_in_OI, fmt.Sprintf("\nThread:%d ,%v , Error in OpenInterest %v, Row: %d  ", ta, f, contents[j][9], j+1))
						}
					}

				}
			}
			wg.Done()
			//tc++
			//log.Printf("Thread closed %d", tc)
			file.Close()
			//log.Println("File closed")
		}(filepath)
	}
	wg.Wait()
	//log.Println("waiting...")
	log.Printf("Errors in Date:\n %v ", error_in_date)
	log.Printf("Errors in Timestamp:\n %v ", error_in_ts)
	log.Printf("Errors in Timestamp Hours:\n %v ", error_in_hours)
	log.Printf("Errors in Timestamp Mins:\n %v", error_in_mins)
	log.Printf("Errors in LTP:\n %v", error_in_LTP)
	log.Printf("Errors in Buy Price:\n %v", error_in_BP)
	log.Printf("Errors in Buy Quantity:\n %v", error_in_BQ)
	log.Printf("Errors in Sell Price:\n %v", error_in_SP)
	log.Printf("Errors in Sell Quantity:\n %v", error_in_SQ)
	log.Printf("Errors in LTQ:\n %v", error_in_LTQ)
	log.Printf("Errors in Open Interest\n %v", error_in_OI)

}
