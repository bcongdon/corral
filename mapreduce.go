package corral

type ValueIterator struct {
	values chan string
}

func (v *ValueIterator) Iter() <-chan string {
	return v.values
}

func newValueIterator(c chan string) ValueIterator {
	return ValueIterator{
		values: c,
	}
}

type Mapper interface {
	Map(key, value string, emitter Emitter)
}

type Reducer interface {
	Reduce(key string, values ValueIterator, emitter Emitter)
}

type keyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
