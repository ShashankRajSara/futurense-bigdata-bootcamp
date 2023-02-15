from mrjob.job import MRJob

class Weather(MRJob):

    def mapper(self, key, line):
        yield('temp_max',float(line.split()[5]))
        yield('temp_min',float(line.split()[6]))
    
    def reducer(self,key, temp):
        if key == 'temp_max':
            yield(key,max(temp))
        else:
            yield(key,min(temp))
        #  if key == 'temp_max' else min(temp))

if __name__ == '__main__':
    Weather.run()