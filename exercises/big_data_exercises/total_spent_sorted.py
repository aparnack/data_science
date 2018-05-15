from mrjob.step import MRStep
from mrjob.job import MRJob

class TotalSpentSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_amount,
                   reducer=self.reducer_total_amount),
            MRStep(mapper=self.mapper_put_zeroes,
                   reducer=self.reducer_give_final)
        ]

    def mapper_get_amount(self,_,line):
        customerID,_,ord_amount=line.split(',')
        yield customerID,float(ord_amount)

    def reducer_total_amount(self,key,val):
        yield key, sum(val)

    def mapper_put_zeroes(self,key,val):
        yield '%04.02f'%int(val),key

    def reducer_give_final(self,amount,customerID) :
        yield  amount,  customerID

if __name__ == '__main__' :
    TotalSpentSorted.run()
