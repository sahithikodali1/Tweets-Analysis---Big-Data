
from mrjob.job import MRJob
from mrjob.step import MRStep

class Bucket_Sort(MRJob):
    
    def steps(self):
        return[
            MRStep(mapper = self.mapper_get_objID,
                   combiner = self.combiner_bucket_sort_objID,
                  reducer = self.reducer_bucket_sort_objID)
        ]

    def mapper_get_objID(self,_, line):
        words = line.split("\t")
        docname = words[0]
        obj_ID = words[1]
        n = 10000
        bucket = (n*float(obj_ID))
        bucket = str(bucket)
        yield (bucket,obj_ID,docname),"Buckets"
        
         
    def combiner_bucket_sort_objID(self,bucket_ID_docname,Buckets):
        (bucket,obj_ID) = bucket_ID_docname[0],bucket_ID_docname[1]
        docname = bucket_ID_docname[2]
        yield(bucket,obj_ID,docname), "Each bucket"
        
        
   
    def reducer_bucket_sort_objID(self,bucket_ID_docname,Bucketsort):
        (bucket,obj_ID) = bucket_ID_docname[0],bucket_ID_docname[1]
        docname = bucket_ID_docname[2]
        yield(obj_ID, docname)
        

if __name__ == '__main__':
    Bucket_Sort.run()
