//
// Copyright (c) 2014 The pblcache Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/pblcache/pblcache/cache"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	KB               = 1024
	MB               = 1024 * KB
	GB               = 1024 * MB
	PblcachedVersion = "(dev)"
)

var (
	cachefilename string
	blocksize     int
	cpuprofile    bool
	cachesavefile string
	segmentsize   int
	directio      bool
)

func init() {
	flag.StringVar(&cachefilename, "cache", "", "Cache file name")
	flag.StringVar(&cachesavefile, "cachemeta", "cache.pbl", "Persistent cache metadata location")
	flag.IntVar(&blocksize, "blocksize", 4, "Cache block size in KB")
	flag.BoolVar(&cpuprofile, "cpuprofile", false, "Create a Go cpu profile for analysis")
	flag.IntVar(&segmentsize, "segmentsize", 512, "Log segment size in KB")
	flag.BoolVar(&directio, "sync", true, "Bypass the buffer page cache")
}

func main() {
	// Gather command line arguments
	flag.Parse()

	// Open stats file
	pbliodata := "blah"
	fp, err := os.Create(pbliodata)
	if err != nil {
		fmt.Print(err)
		return
	}
	metrics := bufio.NewWriter(fp)
	defer fp.Close()

	// Setup number of blocks
	blocksize_bytes := uint32(blocksize * KB)

	// Open cache
	var c *cache.CacheMap
	var log *cache.Log
	var logblocks uint32

	// Show banner
	fmt.Printf("pblcached %s\n", PblcachedVersion)

	// Check segment size
	if segmentsize < (blocksize * 2) {
		fmt.Println("Segment size too small")
		os.Exit(1)
	}
	if segmentsize*KB > 16*MB {
		fmt.Println("Segment size too large")
		os.Exit(1)
	}

	// Determine if we need to use the cache
	if cachefilename == "" {
		fmt.Println("Need a cache file location")
		os.Exit(1)
	}

	// Create log
	log, logblocks, err = cache.NewLog(cachefilename,
		blocksize_bytes,
		(uint32(segmentsize)*KB)/blocksize_bytes,
		0, // buffer cache has been removed for now
		directio,
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if logblocks == 0 {
		fmt.Println("Unable to allocate blocks")
		os.Exit(1)
	}

	// Connect cache metadata with log
	c = cache.NewCacheMap(logblocks, blocksize_bytes, log.Msgchan)
	cache_state := "New"
	if _, err = os.Stat(cachesavefile); err == nil {
		err = c.Load(cachesavefile, log)
		if err != nil {
			fmt.Printf("Unable to load metadata: %s", err)
			return
		}
		cache_state = "Loaded"
	}

	// Start log goroutines
	log.Start()

	// Print banner
	fmt.Printf("Cache   : %s (%s)\n"+
		"C Size  : %.2f GB\n",
		cachefilename, cache_state,
		float64(logblocks)*float64(blocksize_bytes)/GB)

	// Start cpu profiling
	if cpuprofile {
		f, _ := os.Create("cpuprofile")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Shutdown on signal
	quit := make(chan struct{})
	signalch := make(chan os.Signal, 1)
	signal.Notify(signalch, os.Interrupt)
	go func() {
		select {
		case <-signalch:
			close(quit)
			return
		}
	}()

	// Start service
	var servicewg sync.WaitGroup
	servicewg.Add(1)
	go func() {
		defer servicewg.Done()

		time.Sleep(5 * time.Second)
	}()

	// Now we can close the output goroutine
	servicewg.Wait()

	// Print cache stats
	if c != nil {
		c.Close()
		log.Close()
		err = c.Save(cachesavefile, log)
		if err != nil {
			fmt.Printf("Unable to save metadata: %s\n", err)
			os.Remove(cachesavefile)
		}
		fmt.Print(c)
		fmt.Print(log)
	}
	metrics.Flush()

}
