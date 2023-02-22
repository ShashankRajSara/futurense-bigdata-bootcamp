from mrjob.job import MRJob

class yearbyMovies(MRJob):

    def mapper(self, key, line):
        l = line.split(',')
        y = l[-1]

        if y.isnumeric():
            yield(y,1) 
        else:
            pass
        
    def reducer(self,key, count):
            yield(key,sum(count))

if __name__ == '__main__':
    yearbyMovies.run()