from mrjob.job import MRJob

class FriendCounter(MRJob):
    def mapper(self, key, line):
        (userID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)

    def reducer(self, age, numFriends):
        total = sum(numFriends)
        yield age, total

if __name__ == '__main__':
    FriendCounter.run()
