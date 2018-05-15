from mrjob.job import MRJob

class WordFreqCounter(MRJob):
    def mapper(self,_,line):
        words = line.split()
        for word in words:
            yield word.lower(),1

    def reducer(self,word,num1):
        yield word,sum(num1)

if __name__ == '__main__':
    WordFreqCounter.run()
