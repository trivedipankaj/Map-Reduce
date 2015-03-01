from mrjob.job import MRJob
class FindInverted(MRJob):

    def mapper(self, _, line):
	for word in line.split('\n'):
	    w = word.split();
	    yield (w[1], int(w[0]))
    
    def combiner(self, word, counts):
        t=[]
        for id in counts:
            t.append(id)
        yield (word, t)

    def reducer(self, word, counts):
	t=[]
	for id in counts:
	    t.append(id)
	yield (word, t)


if __name__ == '__main__':
    FindInverted.run()
