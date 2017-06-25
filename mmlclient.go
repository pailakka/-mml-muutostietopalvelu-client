package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	"time"

	"encoding/gob"

	"sync"

	"github.com/inconshreveable/log15"
	"github.com/mmcdole/gofeed"
	"github.com/mmcdole/gofeed/atom"
	"github.com/spf13/viper"
)

var apiKey string
var atomBase string

type mmlProduct struct {
	GUID    string
	Updated time.Time
	Title   string
	Formats []string
}

type mmlEntry struct {
	ID              string
	Title           string
	Updated         time.Time
	Link            string
	Size            int64
	Type            string
	DestinationPath string
	DestinationFile string
	Start           time.Time
	DiskSize        int64
}

type cacheKey struct {
	Product string
	Version string
	Format  string
}

type cacheStatus struct {
	Status map[cacheKey]cacheUpdateStatus
}

type cacheUpdateStatus struct {
	GUID         string
	Format       string
	CacheUpdated time.Time
	EntryUpdated map[string]time.Time
}

var entryQueue chan mmlEntry
var entryReady chan mmlEntry
var numWorkers int
var wg sync.WaitGroup

func init() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	apiKey = viper.GetString("api_key")
	atomBase = viper.GetString("atom_url")
	numWorkers = viper.GetInt("num_workers")

	entryQueue = make(chan mmlEntry, 5000)
	entryReady = make(chan mmlEntry)

	for i := 0; i < numWorkers; i++ {
		go entryDownloader(entryQueue, entryReady, &wg)
	}
}

func entryDownloader(dlentry <-chan mmlEntry, readyentry chan<- mmlEntry, twg *sync.WaitGroup) {

	for e := range dlentry {
		e.Start = time.Now()
		df, err := os.Create(path.Join(e.DestinationPath, e.DestinationFile))
		if err != nil {
			df.Close()
			log15.Error(err.Error())
		}

		timeout := time.Duration(120 * time.Second)
		client := http.Client{
			Timeout: timeout,
		}

		resp, err := client.Get(e.Link)

		if err != nil {
			resp.Body.Close()
			log15.Error(err.Error())
		}
		n, err := io.Copy(df, resp.Body)

		if err != nil {
			df.Close()
			resp.Body.Close()
			log15.Error(err.Error())
		}
		e.DiskSize = n
		df.Close()
		resp.Body.Close()

		readyentry <- e

		(*twg).Done()
	}
}

func getAtomURL(product, version string, params map[string]string) *url.URL {

	var feedurl *url.URL
	var err error
	if len(product) == 0 || len(version) == 0 {
		feedurl, err = url.Parse(atomBase)
	} else {
		feedurl, err = url.Parse(fmt.Sprintf("%s/%s/%s", atomBase, product, version))
	}

	if err != nil {
		panic(err)
	}

	q := feedurl.Query()
	q.Add("api_key", apiKey)
	for k, v := range params {
		q.Add(k, v)
	}
	feedurl.RawQuery = q.Encode()

	return feedurl

}
func loadProductsList() (products []mmlProduct, err error) {
	listurl := getAtomURL("", "", map[string]string{})

	log15.Debug("loadProductsList", "listurl", listurl)

	fp := gofeed.NewParser()
	feed, err := fp.ParseURL(listurl.String())

	if err != nil {
		panic(err)
	}

	for _, i := range feed.Items {
		mp := mmlProduct{}
		mp.GUID = i.GUID
		mp.Title = i.Title
		if i.UpdatedParsed != nil {
			mp.Updated = *i.UpdatedParsed
		}
		for _, d := range i.Extensions["nls"]["distributionFormat"] {
			mp.Formats = append(mp.Formats, d.Value)
		}

		products = append(products, mp)
	}

	return products, err

}

func loadUpdatedInfoFromDir(dest string) (updated cacheStatus, err error) {
	updated = cacheStatus{}

	updated.Status = make(map[cacheKey]cacheUpdateStatus)

	updatedFile := path.Join(dest, "updated")

	if _, err := os.Stat(updatedFile); os.IsNotExist(err) {
		return updated, err
	}

	f, err := os.Open(updatedFile)

	if err != nil {
		panic(err)
	}

	dec := gob.NewDecoder(f)

	err = dec.Decode(&updated)

	if err != nil {
		panic(err)
	}
	f.Close()

	return updated, err
}

func saveUpdatedInfoToDir(updated cacheStatus, dest string) (err error) {
	updatedFile := path.Join(dest, "updated")

	f, err := os.Create(updatedFile)

	if err != nil {
		panic(err)
	}

	enc := gob.NewEncoder(f)

	err = enc.Encode(updated)

	if err != nil {
		panic(err)
	}
	f.Close()
	return err
}

func loadProductToDest(product, version, format, dest string, force bool) (err error) {
	updated, err := loadUpdatedInfoFromDir(dest)
	pck := cacheKey{product, version, format}

	status := updated.Status[pck]

	if status.EntryUpdated == nil {
		status.EntryUpdated = make(map[string]time.Time)
	}

	updtime := updated.Status[pck].CacheUpdated

	if force {
		updtime = time.Date(1990, time.June, 20, 0, 0, 0, 0, time.UTC)
	}

	producturl := getAtomURL(product, version, map[string]string{"format": format, "updated": updtime.Format("2006-01-02T15:04:05")}).String()

	status.CacheUpdated = time.Now()

	var entries []mmlEntry
	for {

		log15.Debug("loadProductToDest", "url", producturl)

		timeout := time.Duration(120 * time.Second)
		client := http.Client{
			Timeout: timeout,
		}

		fp := atom.Parser{}
		resp, err := client.Get(producturl)
		if err != nil {
			panic(err)
		}

		feed, err := fp.Parse(resp.Body)

		resp.Body.Close()

		if err != nil {
			panic(err)
		}

		hasNext := false
		for _, l := range feed.Links {
			if l.Rel == "next" {
				hasNext = true
				producturl = strings.Replace(strings.Replace(l.Href, "&amp;", "&", -1), "gml+xml", "gml%2Bxml", -1)
			}
		}

		for _, e := range feed.Entries {
			for _, l := range e.Links {

				me := mmlEntry{}
				me.ID = e.ID
				if l.Title != "" {
					me.Title = l.Title
				} else {
					me.Title = e.Title
				}
				me.Updated = *e.UpdatedParsed
				me.Link = l.Href
				if l.Length != "" {
					me.Size, err = strconv.ParseInt(l.Length, 10, 64)

					if err != nil {
						panic(err)
					}
				}
				me.Type = l.Type

				dpath := strings.Replace(e.ID, "urn:path:", "", -1)
				me.DestinationPath, _ = path.Split(dpath)
				me.DestinationFile = me.Title
				me.DestinationPath = path.Join(dest, me.DestinationPath)
				if _, err := os.Stat(path.Join(me.DestinationPath, me.DestinationFile)); os.IsNotExist(err) {
					entries = append(entries, me)
				} else {
					log15.Info("entry exists", "entry", me.Title, "updated", me.Updated, "took", time.Since(me.Start), "size", me.DiskSize)
				}

			}
		}

		if !hasNext {
			break
		}
	}

	var mutex = &sync.Mutex{}

	go func() {
		for re := range entryReady {
			mutex.Lock()
			status.EntryUpdated[re.ID] = re.Updated
			mutex.Unlock()
			log15.Info("entry ready", "entry", re.Title, "updated", re.Updated, "took", time.Since(re.Start), "size", re.DiskSize)
		}
	}()

	for _, e := range entries {
		mutex.Lock()
		os.MkdirAll(e.DestinationPath, 0755)
		if eOldUpdated, ok := status.EntryUpdated[e.ID]; ok {
			if force || e.Updated.After(eOldUpdated) {
				wg.Add(1)
				entryQueue <- e
			}
		} else {
			wg.Add(1)
			entryQueue <- e
		}
		mutex.Unlock()

	}

	wg.Wait()

	log15.Info("ALL IS DONE")

	updated.Status[pck] = status

	saveUpdatedInfoToDir(updated, dest)
	return err
}
