from mrjob.job import MRJob
class FindMax(MRJob):

    def mapper(self, _, line):
	for word in line.split('\n'):
	    w = word.split();
	    if int(w[1]) > 10:
	        yield (w[0], 1)

    def reducer(self, word, counts):
	yield (word, sum(counts))


if __name__ == '__main__':
    FindMax.run()
