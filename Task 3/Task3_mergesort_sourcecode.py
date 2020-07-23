from mrjob.job import MRJob
from mrjob.step import MRStep

class Merge_Sort(MRJob):
    
    def steps(self):
        return[
            MRStep(mapper = self.mapper_get_objID,
                  reducer = self.reducer_merge_sort_objID)
        ]

    def mapper_get_objID(self,_, line):
        (docname, ID) = line.split("\t")
        ID = ID + ',' + docname
        yield(ID,1)

   
    def reducer_merge_sort_objID(self,ID_docname,Sorted): 
        (ID, docname) = ID_docname.split(",")
        yield ID,docname
        

if __name__ == '__main__':
    Merge_Sort.run()
