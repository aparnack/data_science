from mrjob.job import MRJob

#customer, item, order_amount

class TotalAmountPerCustomer(MRJob):
    def mapper(self,_,line):
        customerID,_,ord_amount=line.split(',')
        yield customerID,float(ord_amount)
    def reducer(self,key,val):
        yield key, sum(val)

if __name__ == '__main__':
    TotalAmountPerCustomer.run()
