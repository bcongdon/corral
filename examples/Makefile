all: word_count

word_count: word_count.go
	go build -o word_count word_count.go

test_word_count: word_count
	./word_count metamorphosis.txt

clean:
	rm -f word_count *.out output*
