package yabf

// Workload represents One experiment scenario.
// One object of this type will be instantiated and
// shared among all client routines.
// This class should be constructed using a no-argument constructor,
// so we can load it dynamically. Any argument-based initialization
// should be done by init().
type Workload interface {
	// Initialize the scenario. Create any generators and other shared
	// objects here.
	// Called once in the main client routine, before any operations
	// are started.
	Init(p Properties) error

	// Initialize any state for a particular client routine.
	// Since the scenario object will be shared among all threads,
	// this is the place to create any state that is specific to one routine.
	// To be clear, this means the returned object should be created anew
	// on each call to InitRoutine(); do not return the same object multiple
	// times. The returned object will be passed to invocations of DoInsert()
	// and DoTransaction() for this routine. There should be no side effects
	// from this call; all state should be encapsulated in the returned object.
	// If you have no state to retain for this routine, return null.
	// (But if you have no state to retain for this routine, probably
	// you don't need to override this function.)
	InitRoutine(p Properties, id int64) (interface{}, error)

	// Cleanup the scenario.
	// Called once, in the main client routine, after all operations
	// have completed.
	Cleanup() error

	// Do one insert operation. Because it will be called concurrently from
	// multiple routines, this function must be routine safe.
	// However, avoid synchronized, or the routines will block waiting for
	// each other, and it will be difficult to reah the target throughput.
	// Ideally, this function would have no side effects other than
	// DB operations and mutations on object. Mutations to object do not need
	// to be synchronized, since each routine has its own object instance.
	DoInsert(db DB, object interface{}) bool

	// Do one transaction operation. Because it will be called concurrently
	// from multiple client routines, this function must be routine safe.
	// However, avoid synchronized, or the routines will block waiting for
	// each other, and it will be difficult to reach the target throughtput.
	// Ideally, this function would have no side effects other than
	// DB operations and mutations on object. Mutations to object do not need
	// to be synchronized, since each routine has its own object instance.
	DoTransaction(db DB, object interface{}) bool

	// Allows scheduling a request to stop the Workload.
	RequestStop()

	// Check the status of the stop request flag.
	isStopRequested() bool
}
