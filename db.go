package yabf

import (
	"errors"
)

var (
	Error              = errors.New("The operation failed.")
	NotFound           = errors.New("The requested record was not found.")
	NotImplemented     = errors.New("The operation is not implemented for the current binding.")
	UnexpectedState    = errors.New("The operation reported success, but the result was not as expected.")
	BadRequest         = errors.New("The request was not valid.")
	Forbidden          = errors.New("The request was not valid.")
	ServiceUnavailable = errors.New("Dependant service for the current binding is not available.")
)

// Binary represents arbitrary binary value(byte array).
type Binary []byte

// Result represents the result type of db operations.
type KVMap map[string]Binary

// DB is A layer for accessing a database to be benchmarked.
// Each routine in the client will be given its own instance of
// whatever DB class is to be used in the test.
// This class should be constructed using a no-argument constructor, so we can
// load it dynamically. Any argument-based initialization should be
// done by Init().
//
// Note that YABF does not make any use of the return codes returned by this class.
// Instead, it keeps a count of the return values and presents them to the user.
//
// The semantics of methods such as Insert, Update and Delete vary from database
// to database.  In particular, operations may or may not be durable once these
// methods commit, and some systems may return 'success' regardless of whether
// or not a tuple with a matching key existed before the call.  Rather than dictate
// the exact semantics of these methods, we recommend you either implement them
// to match the database's default semantics, or the semantics of your
// target application.  For the sake of comparison between experiments we also
// recommend you explain the semantics you chose when presenting performance results.
type DB interface {
	// Initialize any state for this DB.
	// Called once per DB instance; there is one DB instance per client routine.
	Init() error

	// Cleanup any state for this DB.
	// Called once per DB instance; there is one DB instance per client routine.
	Cleanup() error

	// Read a record from the database.
	// Each field/value pair from the result will be returned.
	Read(table string, key Binary, fields []Binary) (KVMap, error)

	// Perform a range scan for a set of records in the database.
	// Each field/value pair from the result will be returned.
	Scan(table string, key Binary, fields []Binary) (KVMap, error)

	// Update a record in the database.
	// Any field/value pairs in the specified values will be written into
	// the record with the specified record key, overwriting any existing
	// values with the same field name.
	Update(table string, key Binary, values KVMap) error

	// Insert a record in the database. Any field/value pairs in the specified
	// values will be written into the record with the specified record key.
	Insert(table string, key Binary, values KVMap) error

	// Delete a reord from the database.
	Delete(table string, key Binary) error
}
