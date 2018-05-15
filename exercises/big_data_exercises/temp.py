from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMinTempFinder(MRJob):

    def conv_fh(self,given_val):
        fh = float(given_val) * 0.18 + 32
        return fh

    def mapper(self,_,line):
        placeID,timestamp,max_or_min,data,_,_,_,_ = line.split(',')
        if(max_or_min == 'TMIN'):
            temp_val = self.conv_fh(data)
            yield placeID, temp_val

    def reducer(self,placeID,temp_val):
        yield placeID, min(temp_val)
if __name__ == '__main__':
    MRMinTempFinder.run()
