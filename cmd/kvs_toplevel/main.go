package main

import (
	"fmt"
	"log"

	"github.com/linxGnu/grocksdb"
	"github.com/o53/etf"
	"github.com/o53/kvs"
)

func main() {
	// Set up RocksDB directory
	// dbPath := filepath.Join(os.TempDir(), "rocksdb_example")
	dbPath := "./rocksdb_example"
	// defer os.RemoveAll(dbPath)

	// Initialize RocksDB
	// Set up RocksDB options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	readOpts := grocksdb.NewDefaultReadOptions()
	writeOpts := grocksdb.NewDefaultWriteOptions()

	// Open RocksDB
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		log.Fatal("Error opening RocksDB:", err)
	}

	rocksdb := kvs.NewRocksDB(db, readOpts, writeOpts)

	// defer rocksdb.Close()

	// Initialize KVSRocksDB
	kvs := kvs.NewKVSRocksDB(rocksdb)

	// Test Seq method
	seqID, err := kvs.Seq()
	if err != nil {
		log.Fatal("Error in Seq:", err)
	}
	fmt.Println("Next Sequence ID:", seqID)

	// Test Put method
	id := seqID
	data := etf.Map{
		etf.MapElem{Key: etf.Atom("name"), Value: etf.Atom("Alice")},
		etf.MapElem{Key: etf.Atom("age"), Value: etf.Integer(30)},
	}
	err = kvs.Put(id, data)
	if err != nil {
		log.Fatal("Error in Put:", err)
	}
	fmt.Printf("Inserted data with ID %v\n", id)

	// Test Get method
	retrievedData, err := kvs.Get(id)
	if err != nil {
		log.Fatal("Error in Get:", err)
	}
	fmt.Printf("Retrieved Data for ID %v: %v\n", id, retrievedData)

	// Test Index method
	matchedKeys, err := kvs.Index(etf.Atom("name"), etf.Atom("Alice"))
	if err != nil {
		log.Fatal("Error in Index:", err)
	}
	fmt.Printf("Keys matching name='Alice': %v\n", matchedKeys)

	// Test Count method
	count, err := kvs.Count()
	if err != nil {
		log.Fatal("Error in Count:", err)
	}
	fmt.Println("Total number of entries:", count)

	// Test Dir method
	keys, err := kvs.Dir()
	if err != nil {
		log.Fatal("Error in Dir:", err)
	}
	fmt.Println("All keys in the database:")
	for _, key := range keys {
		fmt.Println(key)
	}

	// Test Delete method
	err = kvs.Delete(id)
	if err != nil {
		log.Fatal("Error in Delete:", err)
	}
	fmt.Printf("Deleted data with ID %v\n", id)

	// Verify deletion
	_, err = kvs.Get(id)
	if err != nil {
		fmt.Printf("Data with ID %v successfully deleted.\n", id)
	} else {
		fmt.Printf("Error: Data with ID %v was not deleted.\n", id)
	}

	// Test Count method after deletion
	count, err = kvs.Count()
	if err != nil {
		log.Fatal("Error in Count:", err)
	}
	fmt.Println("Total number of entries after deletion:", count)
}
