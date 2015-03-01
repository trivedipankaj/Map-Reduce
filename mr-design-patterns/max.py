from mrjob.job import MRJob
class FindMax(MRJob):

    def mapper(self, _, line):
        t ={}
	for word in line.split('\n'):
	    w = word.split();
	    yield (w[0], w[1])

    def reducer(self, word, counts):
	yield (word, max(counts))


if __name__ == '__main__':
    FindMax.run()
