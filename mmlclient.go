package main

import (
	"archive/zip"
	"errors"
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

	"github.com/mmcdole/gofeed"
	"github.com/mmcdole/gofeed/atom"
	log "github.com/sirupsen/logrus"
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
	ID               string
	Title            string
	Updated          time.Time
	Link             string
	Size             int64
	Type             string
	DestinationPath  string
	DestinationFile  string
	Start            time.Time
	DiskSize         int64
	UncompressedSize int64
	RetryCount       int16
	Exists           bool
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
	viper.SetConfigName("mmlclient")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	apiKey = viper.GetString("api_key")
	atomBase = viper.GetString("atom_url")
	numWorkers = viper.GetInt("num_workers")

	entryQueue = make(chan mmlEntry, 500000)
	entryReady = make(chan mmlEntry)

	for i := 0; i < numWorkers; i++ {
		go entryDownloader(entryQueue, entryReady, &wg)
	}
}

func entryDownloader(dlentry chan mmlEntry, readyentry chan<- mmlEntry, twg *sync.WaitGroup) {

	for e := range dlentry {
		e.Start = time.Now()
		destinationFilename := path.Join(e.DestinationPath, e.DestinationFile)
		df, err := os.Create(destinationFilename)
		if err != nil {
			df.Close()
			log.Error(err.Error())
		}

		timeout := 120 * time.Second
		client := http.Client{
			Timeout: timeout,
		}

		resp, err := client.Get(e.Link)

		if err != nil {
			if resp != nil {
				resp.Body.Close()
			}
			log.Error(err.Error())
		}
		n, err := io.Copy(df, resp.Body)

		if err != nil {
			df.Close()
			resp.Body.Close()
			log.Error(err.Error())
		}
		e.DiskSize = n
		df.Close()
		resp.Body.Close()

		uncompSize, err := verifyZipfile(destinationFilename)
		e.UncompressedSize = uncompSize
		if err != nil {
			logCtx := log.WithFields(log.Fields{
				"retrycount": e.RetryCount,
				"filename":   destinationFilename,
				"error":      err,
			})
			err = os.Remove(destinationFilename)
			if err != nil {
				logCtx.Error("Error deleting file")
			}

			if e.RetryCount < 5 {
				logCtx.Error("Failed to verify zipfile, retrying")
				e.RetryCount = e.RetryCount + 1
				dlentry <- e
			} else {
				logCtx.Error("Failed to verify zipfile, deleting")
				(*twg).Done()
			}
		} else {
			readyentry <- e
			(*twg).Done()
		}

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

	log.WithField("listurl", listurl).Debug("loadProductsList")

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
		return updated, nil
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

func loadProductToDest(product, version, format, dest string, force, onlymissing bool, fromdate string) (err error) {
	updated, err := loadUpdatedInfoFromDir(dest)
	pck := cacheKey{product, version, format}

	status := updated.Status[pck]

	if status.EntryUpdated == nil {
		status.EntryUpdated = make(map[string]time.Time)
	}

	updtime := updated.Status[pck].CacheUpdated

	if force {
		updtime, err = time.Parse(time.RFC3339, fromdate)
		if err != nil {
			log.WithField("fromdata", fromdate).Error("Unable to parse forced from date")
			panic(err)
		}

	}
	log.Debug("loadProductToDest", "updtime", updtime)

	producturl := getAtomURL(product, version, map[string]string{"format": format, "updated": "1990-06-20T00:00:00"}).String()

	status.CacheUpdated = time.Now()

	var entries []mmlEntry
	for {

		log.WithFields(log.Fields{
			"nentries": len(entries),
			"url":      producturl,
		}).Debug("loadProductToDest")

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

				_, statErr := os.Stat(path.Join(me.DestinationPath, me.DestinationFile))
				fileExists := !os.IsNotExist(statErr)

				if fileExists {
					logCtx := log.WithFields(log.Fields{
						"path": path.Join(me.DestinationPath, me.DestinationFile),
					})
					if onlymissing {
						logCtx.Debug("loadProductToDest ", "entry exists")
						continue
					}
					if me.Updated.Before(updtime) {
						logCtx.Debug("loadProductToDes t", "entry not updated")
						continue
					}
					me.Exists = true
				}

				entries = append(entries, me)

			}
		}

		if !hasNext {
			break
		}
	}

	log.WithField("nentries", len(entries)).Info("loadProductToDest entries done")

	var mutex = &sync.Mutex{}

	go func() {
		for re := range entryReady {
			mutex.Lock()
			status.EntryUpdated[re.ID] = re.Updated
			mutex.Unlock()
			log.WithFields(log.Fields{
				"entry":        re.Title,
				"updated":      re.Updated,
				"took":         time.Since(re.Start),
				"size":         re.DiskSize,
				"uncompressed": re.UncompressedSize,
			}).Info("entry ready")
		}
	}()

	for _, e := range entries {
		if _, err := os.Stat(e.DestinationPath); os.IsNotExist(err) {
			os.MkdirAll(e.DestinationPath, 0755)
			log.WithFields(log.Fields{
				"path": e.DestinationPath,
			}).Info("path created")
		}

		mutex.Lock()
		eOldUpdated, ok := status.EntryUpdated[e.ID]
		mutex.Unlock()
		if ok {
			if force || e.Updated.After(eOldUpdated) || !e.Exists {
				wg.Add(1)
				entryQueue <- e
			}
		} else {
			wg.Add(1)
			entryQueue <- e
		}
	}

	wg.Wait()

	log.Info("ALL IS DONE, len: ", len(entryQueue))

	updated.Status[pck] = status
	mutex.Lock()
	saveUpdatedInfoToDir(updated, dest)
	mutex.Unlock()
	return err
}

func verifyZipfile(filename string) (uncompSize int64, err error) {
	// Open a zip archive for reading.
	r, err := zip.OpenReader(filename)
	if err != nil {
		return uncompSize, err
	}
	defer r.Close()

	for _, f := range r.File {
		uncompSize += f.FileInfo().Size()
	}

	if uncompSize < 10 {
		return uncompSize, errors.New("Too small file to be correct.")
	}
	return uncompSize, err
}
