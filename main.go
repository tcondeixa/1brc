package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
)

// [0] = min
// [1] = mac
// [2] = samples
// [3] = sum
type Measures = [4]float64
type City = string

func main() {

	// f, err := os.Create("cpu.prof")
	// if err != nil {
	// 	panic(err)
	// }
	// defer f.Close()
	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	panic(err)
	// }
	// defer pprof.StopCPUProfile()

	var m = make(map[City]Measures)

	a, err := os.ReadFile("measurements.txt")
	if err != nil {
		panic(err)
	}

	numRoutines := 256
	c := make(chan map[City]Measures, numRoutines)
	var wg sync.WaitGroup

	chunkSize := len(a) / numRoutines
	startChunk := 0
	endChunk := chunkSize
	for {
		for i := 0; i < chunkSize; i++ {
			endChunk = endChunk + i
			if a[endChunk] == '\n' {
				wg.Add(1)
				go func() {
					defer wg.Done()
					processChunk(c, a[startChunk:endChunk+1])
				}()
				break
			}
		}

		startChunk = endChunk + 1
		endChunk = startChunk + chunkSize
		if endChunk >= len(a) {
			endChunk = len(a) - 1
		}

		if startChunk >= len(a) {
			break
		}
	}

	go func() {
		wg.Wait()
		close(c)
	}()

	for p := range c {
		for k, r := range p {
			val, ok := m[k]
			if !ok {
				m[k] = Measures{r[0], r[1], r[2], r[3]}
			} else {
				if r[0] < val[0] {
					val[0] = r[0]
				}

				if r[1] > val[1] {
					val[1] = r[1]
				}

				val[2] = val[2] + r[2]
				val[3] = val[3] + r[3]
				m[k] = val
			}
		}
	}

	orderCities := []City{}
	for k := range m {
		orderCities = append(orderCities, k)
	}
	slices.Sort(orderCities)

	fmt.Print("{")
	for i, city := range orderCities {
		fmt.Printf("%s=%.1f/%.1f/%.1f", city, m[city][0], m[city][3]/m[city][2], m[city][1])
		if i < len(orderCities)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Println("}")
}

func processChunk(c chan map[City]Measures, chunk []byte) {

	m := make(map[City]Measures)
	newIndex := 0
	splitIndex := 0
	for i := 0; i < len(chunk); i++ {
		if chunk[i] == ';' {
			splitIndex = i
		} else if chunk[i] == '\n' {
			measure, err := strconv.ParseFloat(string(chunk[splitIndex+1:i]), 32)
			if err != nil {
				panic(err)
			}
			city := string(chunk[newIndex:splitIndex])
			val, ok := m[city]
			if ok {
				if measure < val[0] {
					val[0] = measure
				}

				if measure > val[1] {
					val[1] = measure
				}

				val[2]++
				val[3] = val[3] + measure
				m[city] = val

			} else {
				m[city] = [4]float64{measure, measure, 1, 1}
			}
			newIndex = i + 1
		}
	}

	c <- m
}
