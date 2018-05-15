from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularHero(MRJob):

    # Defining  a function to add arguments.
    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--names', help='Path to Marvel-Names.txt')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_friends,
                   reducer=self.reducer_count_friends),
            MRStep(mapper = self.mapper_prep_for_sort,
                   mapper_init=self.mapper_init,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_friends(self, _, line):
        heroes = line.split()
        heroID=int(heroes[0])
        num_friends= int(len(heroes)-1)
        yield heroID, num_friends

    def mapper_init(self):
        self.heronames = {}

        with open("Marvel-names.txt", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('"')
                heroID = int(fields[0])
                self.heronames[heroID] = fields[1]

    def reducer_count_friends(self, heroID, numFriendsperLine):
        yield heroID,sum(numFriendsperLine)

    ## For sorting, make key none, and key, value pair as a tuple, with whatever
    ## needs to be sorted as first thing
    def mapper_prep_for_sort(self, heroID,totalFriends):
        heroName = self.heronames[heroID]
        yield None,(totalFriends,heroName)

    def reducer_find_max(self,heroname,totalFriends):
        yield max(totalFriends)


if __name__ == '__main__':
    MostPopularHero.run()
