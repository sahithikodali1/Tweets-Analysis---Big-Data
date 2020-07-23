from mrjob.job import MRJob
from mrjob.step import MRStep
from math import log

no_of_docs = 10000

class tfidf(MRJob):
    
    def steps(self):
        return[
            MRStep(mapper = self.mapper_get_words_per_doc,
                  reducer = self.reducer_count_words_per_doc),
            
            MRStep(mapper = self. mapper_get_tf_per_doc,
                  reducer = self.reducer_count_tf_per_doc),
            
            
            MRStep(mapper = self.mapper_get_idf_freq_in_corpus,
                  reducer = self.reducer_count_idf_freq_in_corpus),
            
            MRStep(mapper = self.tfidf,),
            
        ]

    def mapper_get_words_per_doc(self,_, line):
        words = line.split()
        doc_name = words[0]
        for word in words[1:]:
            word = word.strip()
            word = word + ',' + doc_name
            yield (word,1)
            
    
    def reducer_count_words_per_doc(self,word,count): 
        yield (word,sum(count))
        
    def mapper_get_tf_per_doc(self,word,count):
        word = word.split(",")
        word_freq = word[0] + ',' + str(count)
        yield (word[1],word_freq)
        
    def reducer_count_tf_per_doc(self,doc_name, word_freq):
        words = []
        freq = []
        word_count_per_doc = 0
        for word in word_freq:
            (word,count) = word.split(",") 
            words.append(word)
            freq.append(count)
            word_count_per_doc = str(int(word_count_per_doc) + int(count))
        
        for i in range(len(words)):
            yield ((words[i],doc_name),(freq[i],word_count_per_doc))
    
    def mapper_get_idf_freq_in_corpus(self, word_doc, freq_words_per_doc):
        (word, doc_Name) = word_doc[0], word_doc[1]
        (freq, word_count_per_doc) =  freq_words_per_doc[0], freq_words_per_doc[1]
        yield (word,(doc_Name,freq,word_count_per_doc,1))
        
    def reducer_count_idf_freq_in_corpus(self,word,docName_freq_words):
        total_docs_count = 0
        doc_name = []
        freq = [] 
        word_count_per_doc =[]
        
        for line in docName_freq_words:
            doc_name.append(line[0])
            freq.append(line[1])
            word_count_per_doc.append(line[2])
            total_docs_count += 1
        
        for i in range(len(doc_name)):
            yield (word, doc_name[i]), (freq[i], word_count_per_doc[i],
                                        total_docs_count)
            
    def tfidf(self, word_doc, freq_wordcount_totalcount):
        tf = int(freq_wordcount_totalcount[0])/int(freq_wordcount_totalcount[1])
        idf = log(no_of_docs/(int(freq_wordcount_totalcount[2]) +1))
        tf_idf = tf*idf
        if word_doc[0] == ('health' or 'Health' or 'HEALTH'):
            yield((word_doc[0],word_doc[1]),tf_idf)
        

if __name__ == '__main__':
    tfidf.run()
