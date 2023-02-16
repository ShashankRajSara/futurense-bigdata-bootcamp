from mrjob.job import MRJob
import calendar

class Weather(MRJob):

    def mapper(self, key, line):
        yield(calendar.month_name[int(line.split()[1][4:6])],float(line.split()[5]))
    
    def reducer(self,key, temp):
        yield(key,max(temp),min(temp))


if __name__ == '__main__':
    Weather.run()