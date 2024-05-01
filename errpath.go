package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

func main() {
	input_path := "/home/ajeeb/sandbox/src/ErrorFiles/APR_2021/GFDLNFO_TICK_01042021" //os.Args[1]
	CheckPath(input_path)
}

func CheckPath(inPath string) {
	var wg sync.WaitGroup
	input_path := inPath
	paths := []string{}
	filepath.Walk(input_path, func(path string, info os.FileInfo, err error) error {
		if err == nil && strings.Contains(info.Name(), ".csv") {
			paths = append(paths, path)
		}
		return nil
	})
	//ta := 0
	//tc := 0
	regex_unverified := []string{}
	mismatched_ext := []string{}
	for _, filepath := range paths {
		wg.Add(1)
		//ta++
		//log.Printf("threads opened: %d", ta)
		go func(filepath string) {
			//log.Println("entered go func")
			path := strings.Split(filepath, "/")
			//fmt.Println(len(path))
			switch len(path) {
			case 10:
				//It is an option file
				//log.Println(("case 10"))
				split_path := strings.Split(path[len(path)-1], ".")
				if split_path[1] == "NFO" {
					t, m := useRegex(split_path[0], 10)
					if t {
						// log.Println("Regex verified: option")
						break
					} else {
						regex_unverified = append(regex_unverified, fmt.Sprintf("\n%v , Regex Unverified: %v ", filepath, m))
					}
				} else {
					mismatched_ext = append(mismatched_ext, fmt.Sprintf("\n%v , Mismatched extensions ", filepath))
				}

			case 11:
				//It is a future file
				//log.Println(("case 11"))
				split_path := strings.Split(path[len(path)-1], ".")
				if split_path[1] == "NFO" {
					t, m := useRegex(split_path[0], 11)
					if t {
						// log.Println("Regex verified: future")
						break
					} else {
						regex_unverified = append(regex_unverified, fmt.Sprintf("\n%v , Regex Unverified: %v ", filepath, m))
					}
				} else {
					mismatched_ext = append(mismatched_ext, fmt.Sprintf("\n%v , Mismatched extensions ", filepath))
				}
			}
			wg.Done()
			//tc++
			//log.Printf("Thread Closed : %d", tc)
		}(filepath)
	}

	wg.Wait()
	log.Printf("Regex Unverified: %v", regex_unverified)
	log.Printf("Mismatched Extensions: %v", mismatched_ext)
}

func useRegex(s string, c int) (bool, string) {
	if c == 10 {
		re := regexp.MustCompile("(?i)[A-Za-z]+(0?[1-9]|[12][0-9]|3[01])[A-Za-z]+[0-9]+[A-Za-z]+")
		m := fmt.Sprintf("%q", re.Find([]byte(s)))
		return re.MatchString(s), m
	} else {
		re := regexp.MustCompile("(?i)[A-Za-z]+-[A-Za-z0-9]+")
		m := fmt.Sprintf("%q", re.Find([]byte(s)))
		return re.MatchString(s), m
	}
}
