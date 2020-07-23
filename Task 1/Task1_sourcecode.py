from mrjob.job import MRJob
from mrjob.step import MRStep

class WordFreqCount(MRJob):
    
    def steps(self):
        return[
            MRStep(mapper = self.mapper_get_words,
                  reducer = self.reducer_count_words)
        ]

    def mapper_get_words(self,_, line):
        words = line.split()
        line_no = words[0]
        for word in words[1:]:
            word = word.strip()
            key = word + ',' + line_no
            yield key,1
            
    
    def reducer_count_words(self,word,count): 
        count = sum(count)
        yield (word,count)

if __name__ == '__main__':
    WordFreqCount.run()
