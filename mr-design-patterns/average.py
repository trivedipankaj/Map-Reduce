from mrjob.job import MRJob
class FindAvg(MRJob):

    def mapper(self, _, line):
	for word in line.split('\n'):
	    w = word.split();
	    yield (w[0], int(w[1]))

    def reducer(self, word, counts):
	cnt=0
	s=0
	for t in counts:
	    cnt +=1
	    s +=t
	avg=s/cnt 
	yield (word, avg)


if __name__ == '__main__':
    FindAvg.run()
