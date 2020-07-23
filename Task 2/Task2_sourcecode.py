from mrjob.job import MRJob
from mrjob.step import MRStep

cities = open('cities.txt').read()

Aus_cities = []
Aus_cities.append(cities)
Aus_cities = Aus_cities[0].split("\n")

class CitiesAus_Count(MRJob):

    def steps(self):
        return[
            MRStep(mapper = self.mapper_get_cities_Australia,
                  reducer = self.reducer_count_cities)
        ]

    def mapper_get_cities_Australia(self, key, line):
        pairs = {}
        words = line.split("\t")
        pairs[words[0]] = words[1]
        for city in Aus_cities:
            if (city == words[0] and ('Australia' or city in pairs[words[0]])):
                yield(words[0],1)
    
    def reducer_count_cities(self, word,count):
        yield word,sum(count)
        
            
if __name__ == '__main__':
    CitiesAus_Count.run()
