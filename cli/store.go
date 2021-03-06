package main

import (
	"path/filepath"

	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/boltdb"
	"github.com/chop-dbhi/origins/storage/memory"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Commands can call this if it requires use of the store.
func initStore() *storage.Store {
	var (
		err    error
		store  *storage.Store
		engine storage.Engine
	)

	cf := viper.ConfigFileUsed()
	dir := filepath.Dir(cf)

	// Get path relative to config file.
	path := filepath.Join(dir, viper.GetString("path"))

	opts := storage.Options{
		Path: path,
	}

	switch viper.GetString("storage") {
	case "boltdb":
		engine, err = boltdb.Open(&opts)
	case "memory":
		engine, err = memory.Open(&opts)
	default:
		logrus.Fatal("no storage selected")
	}

	if err != nil {
		logrus.Fatal(err)
	}

	// Initialize a store.
	store, err = storage.Init(&storage.Config{
		Engine: engine,
	})

	if err != nil {
		logrus.Fatal(err)
	}

	return store
}
