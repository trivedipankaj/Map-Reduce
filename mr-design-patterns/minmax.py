from mrjob.job import MRJob
class FindMinMax(MRJob):

    def mapper(self, _, line):
        t ={}
	for word in line.split('\n'):
	    w = word.split();
	    t['min']=int(w[1])
	    t['max']=int(w[1])
	    t['cnt']=1
	    yield (w[0], t)

    def reducer(self, word, counts):
	t_min=10000000
	t_max=-1
	t_cnt=0
	t={}
	for t in counts:
      	    if t['min']<t_min:
		t_min =t['min']
	    if t['max']>t_max:
		t_max=t['max']
	    t_cnt += t['cnt']
	t['min']=t_min
	t['max']=t_max
	t['cnt']=t_cnt
	yield (word, t)


if __name__ == '__main__':
    FindMinMax.run()
