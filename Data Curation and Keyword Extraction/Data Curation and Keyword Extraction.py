#!/usr/bin/env python
# coding: utf-8

# # ASSIGNMENT 1 PART 2
# ### Author : Sahithi Kodali
# ### Student ID : 45712050
# 

#Problem Statement:
#Program in Python, to read the Tweet Dataset (in part 1) from MongoDB and extract keywords from
#each text of the Tweets and add a new name/value pair to the keywords in a comma-separated value (CSV) 
#format; and update the original Tweet in the MongoDB.

# In[ ]:


#IMPORT LIBRARIES

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#importing the library json required to import json file and pymongo from which Mongoclient is imported  
#to communicate with Mongodbimport json
import pymongo as pm
from pymongo import MongoClient

#import the library re and sub from it to use the regular expressions. 
import re
from re import sub

#Import library nltk(Natural Language toolkit) to build Python programs that work with Natural Language processing.
import nltk

# PorterStemmer to stem the unnecessary words, letters and stopwords of english to get rid of them in the text we are analyzing.
from nltk .stem.porter import PorterStemmer
nltk.download('stopwords')
from nltk.corpus import stopwords


# In[ ]:


#READING INPUT FROM MONGODB

#Access the Mongodb client through the link used to connect the cluster 
client = MongoClient("mongodb+srv://Sahi1:1234abcd@clustersahi-ntxpk.mongodb.net/test?retryWrites=true&w=majority")

#add the database
db = client.test

#add the collection
collection = db.Tweets

#Add the collection to the dataframe in the form of list
collection = pd.DataFrame(list(collection.find()))


#printing the column ‘text’ that needed to be worked on
print(collection['text'])

#remove the empty rows in the column to analyze the text
collection = collection.dropna(subset = ['text'])


# In[ ]:


#CLEANING AND TOKENIZING TEXT
#create a new list to add the text that is cleaned
corpus = []
#iterating the text column of our collection
for i in range(len(collection)):
    #regular expression to remove all the other letters and symbols apart from letters and numbers
    Text = re.sub('[^a-zA-Z0-9]',' ',collection['text'][i])
    #Changing the words into lowercase and splitting the texts string into list of lists 
    Text = Text.lower()
    Text = Text.split()
    #Initializing the porter stemmer to a variable to use it to stem each word in text 
    #add if not in the stopwords and join them with space and append all the text in the list created ‘corpus’
    ps = PorterStemmer()
    Text = [ps.stem(word) for word in Text 
            if not word in set(stopwords.words('english'))]
    Text= ' '.join(Text)
    corpus.append(Text)
#Splitting each word of Sentences in the ‘Corpus’ list and storing as tokens
tokens = [sub.split() for sub in corpus]
#Downloading Averaged Perceptron Tagger to tag Parts of Speech for each token obtained
nltk.download('averaged_perceptron_tagger')
#create  a new list to store the tagged tokens
tagged_tokens = []
#iterate through the tokens and use nltk ‘pos_tag’ for tagging the words and append them to the new list
for i in tokens:
    tags = nltk.pos_tag(i)
    tagged_tokens.append(tags)
print(tagged_tokens)


# In[ ]:


#EXTRACTION OF KEYWORDS
 
#Extracting keywords which are tagged as ‘NN’(Nouns) from the list of list of tagged tokens tuples 
keywords = [[tup[0] for tup in list if tup[1] == 'NN'] for list in tagged_tokens]
df = pd.DataFrame(keywords)


#Converting the data frame into a CSV file
df.to_csv("Tweets_csv.csv", index = False , header = True)
print(df)


# In[ ]:


#UPDATING THE DATASET ON MONGODB

db_default = db['Tweets']

#Read the csv file
data = pd.read_csv('Tweets_csv.csv')

#loading the CSV file as a JSON file in record orientation
data_json = json.loads(data.to_json(orient = 'records'))

#deleting the previous data in the collection in Mongodb
db_default.delete_many({})


#Update/Insert the new data into the collection in Mongodb
db_default.insert_many(data_json)


# In[ ]:




