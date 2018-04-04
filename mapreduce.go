package corral

type Emitter interface {
	Emit(key, value string)
}

type ValueIterator interface {
	Iter() <-chan string
}

type Mapper interface {
	Map(key, value string, emitter Emitter)
}

type Reducer interface {
	Reduce(key string, values ValueIterator, emitter Emitter)
}
