package corral

// ValueIterator iterates over a sequence of values.
// This is used during the Reduce phase, wherein a reduce task
// iterates over all values for a particular key.
type ValueIterator struct {
	values chan string
}

// Iter iterates over all the values in the iterator.
func (v *ValueIterator) Iter() <-chan string {
	return v.values
}

func newValueIterator(c chan string) ValueIterator {
	return ValueIterator{
		values: c,
	}
}

// Mapper defines the interface for a Map task.
type Mapper interface {
	Map(key, value string, emitter Emitter)
}

// Reducer defines the interface for a Reduce task.
type Reducer interface {
	Reduce(key string, values ValueIterator, emitter Emitter)
}

// keyValue is used to store intermediate shuffle data as key-value pairs
type keyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
