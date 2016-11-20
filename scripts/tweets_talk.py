#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2015 Mariluz Congosto
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

import os
import re
import sys
import time
import datetime
from datetime import timedelta
import unicodedata
import math
import codecs
import gzip
import argparse


def strip_accents(s):
   return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))


# # A dinamic matrix
# # This matrix is a dict whit only cells it nedeed
# # Column and row numbers start with 1
#  
class Matrix(object):
  def __init__(self,  rows,cols):
    self.matrix={}
    self.cols = int(cols)
    self.rows = int(rows)
 
  def setitem(self, row, col, v):
    self.matrix[row-1,col-1]=v
    return

  def getitem(self, row,col):
    if (row-1,col-1) not in self.matrix:
      return 0
    else:
      return self.matrix[row-1,col-1]

  def __iter__(self):
    for row in range(self.rows):
      for col in range(self.cols):
        yield (self.matrix, row, col) 
        
  def __repr__(self):
    outStr = ""
    for i in range(self.rows):
      for j in range(self.cols):
        if (i,j) not in self.matrix:
          outStr += '0.00,'
        else: 
          outStr += '%.2f,' % ( self.matrix[i,j])
      outStr += '\n'  
    return outStr      
    
class Rank(object):
  def __init__(self):
    self.rank={}
  def set_item(self, item,value):
    if item not in self.rank:
      self.rank[item] = value
    else:
      self.rank[item] = self.rank[item] +value     
    return
    
  def get_item(self, item):
    if item in self.rank:
      return  self.rank[item]
    else:
      return 0

class Sentence_similarity (object):
  def __init__(self,dir_in,prefix,size_sentences):
    self.size_sentences=size_sentences
    self.dir_in=dir_in
    self.prefix=prefix
    self.rank={}
    self.list_texts=[]
    self.dict_id_tweets={}
    self.dict_sentences={}
    self.dict_sentences_count={}
    self.dict_date={}
    self.last_sentence_day =0

  def set_item(self, words,text_source,author,author_source,date,id_tweet):
    found = False
    set_words = frozenset(words) 
    len_set_words = len ( set_words )
    if author_source==None:  #new sentence  
      self.list_texts.append((text_source,author)) 
      self.dict_id_tweets[text_source,author]=id_tweet
      self.dict_sentences[text_source,author]=words
      self.dict_sentences_count[text_source,author]=1
      self.dict_date [text_source,author]=date
      return
    else: #find similarity
      for (text,author_actual)  in self.list_texts:
        if author_source == author_actual: 
          sentence=self.dict_sentences[text,author_actual]
          set_sentence = frozenset(sentence)
          if (abs((len_set_words) - len (set_sentence))) < 4:
            if (len (set_words & set_sentence)/float(len_set_words)) > 0.9 :
              self.dict_sentences_count[text,author_actual] += 1        
              found = True
              break
      if not found:  #Maybe a RT with out original tweet  
        self.list_texts.append((text_source,author_source)) 
        self.dict_id_tweets[text_source,author_source]=id_tweet
        self.dict_sentences[text_source,author_source]=words
        self.dict_sentences_count[text_source,author_source]=1
        self.dict_date [text_source,author_source]=date
    return   
  
  def set_hour(self):   
    #remove sentences not frequent
    list_texts_aux=[]   
    dict_id_tweets_aux={}
    dict_sentences_aux={}
    dict_sentences_count_aux={}
    dict_date_aux={}
    sentences_rank_order=sorted([(value,key) for (key,value) in self.dict_sentences_count.items()],reverse=True) 
    num_sentences=0
    print len (sentences_rank_order)
    for (value,key) in sentences_rank_order:
      (text, author)=key
      num_sentences += 1
      if num_sentences > (self.size_sentences * 2):
        break
      list_texts_aux.append((text,author))
      dict_id_tweets_aux[text,author]= self.dict_id_tweets[text,author]
      dict_sentences_aux[text,author] = self.dict_sentences[text,author]
      dict_sentences_count_aux[text,author] = self.dict_sentences_count[text,author]
      dict_date_aux[text,author] =self. dict_date[text,author]
    print 'Num sentences stored', len(list_texts_aux)    , len(dict_sentences_count_aux)
    self.list_texts[:]
    self.dict_id_tweets.clear()
    self.dict_sentences.clear()
    self.dict_sentences_count.clear()
    self.dict_date.clear()
    
    self.list_texts = list_texts_aux
    self.dict_id_tweets=dict_id_tweets_aux  
    self.dict_sentences = dict_sentences_aux
    self.dict_sentences_count = dict_sentences_count_aux
    self.dict_date = dict_date_aux
    print 'Num sentences stored', len(self.list_texts),len(self.dict_sentences_count)
    return 
    
  def set_day(self,day):   
    #remove sentences not frequent
    list_texts_aux=[]  
    dict_id_tweets_aux={}
    dict_sentences_aux={}
    dict_sentences_count_aux={}
    dict_date_aux={}
    print 'set day', len(self. dict_sentences)
    sentences_rank_order=sorted([(value,key) for (key,value) in self.dict_sentences_count.items()],reverse=True) 
    num_sentences=0
    for (value,key) in sentences_rank_order:
      (text, author)=key
      num_sentences += 1
      if num_sentences > self.size_sentences:
        break
      list_texts_aux.append((text,author))
      dict_id_tweets_aux[text,author]= self.dict_id_tweets[text,author]
      dict_sentences_aux[text,author] = self.dict_sentences[text,author]
      dict_sentences_count_aux[text,author] = self.dict_sentences_count[text,author]
      dict_date_aux[text,author] =self. dict_date[text,author]
         
    self.list_texts[:]
    self.dict_id_tweets.clear()
    self.dict_sentences.clear()
    self.dict_sentences_count.clear()
    self.dict_date.clear()
    
    self.list_texts = list_texts_aux
    self.dict_id_tweets=dict_id_tweets_aux 
    self.dict_sentences = dict_sentences_aux
    self.dict_sentences_count = dict_sentences_count_aux
    self.dict_date= dict_date_aux
    print 'Num sentences stored', len(self.list_texts)   , len(self.dict_sentences_count)
    return 
    
  def get_sentences(self):
    return self.dict_sentences
      
  def get_sentences_count (self):
    return self.dict_sentences_count
    
  def get_texts (self):
    return  self.list_texts
  
  def get_id_tweets (self):
    return  self.dict_id_tweets
      
  def get_num_sentences (self):  
    return len(self.list_texts)
    
  def get_dict_date (self):  
    return self.dict_date
    
  def get_dict_sentences (self):  
    return self.dict_sentences
   
  def reset_sentences (self):  
    print 'cleaned day'
    self.rank.clear()
    self.list_texts[:]
    self.dict_authors.clear()
    self.dict_sentences.clear()
    self.dict_sentences_count.clear()
    self.list_texts[:]
    self.last_sentence_day =0
    return 
  
def token_words (source):
  list_words=[]
  source_without_urls=u''
  #renove urls from tweet
  urls=re.findall (r'(http[s]*://\S+)', source,re.U)
  for url in urls:
    start=source.find(url)
    end=len(url)
    source_without_urls=source_without_urls+source[0:start-1]
    source=source[start+end:] 
  source_without_urls=source_without_urls+source
  list_tokens=re.findall (r'[@#]*\w+', source_without_urls,re.U) 
#  remove users and hashtags
  for token in list_tokens:
    if (token.find(u'#') == -1) and (token.find(u'@') == -1):
      token=token.lower()
      list_words.append(token)
  return list_words

def token_words_url (source):
  list_words=[]
  list_tokens=re.findall (r'\S+', source,re.U) 
#  remove users and rts
  for token in list_tokens:
    if (token != u'rt') and (token !=u'vía') and (token != u'via'):
      list_words.append(token)
  return list_words  

def get_tweet_source (text):
  source=None
  text_aux=text
  start=text_aux.find('RT')
  while  start !=  -1:
    #print start
    text=text_aux[start:]
    #print text
    RT= re.match('[RT[\s]*(@\w+)[:]*',text,re.U)
    if RT:
      source=RT.group(1)
      text_aux=text[len(RT.group(0)):]
      #print text_aux
      #print source
      start=text_aux.find('RT')
    else:
      break
  return (source, text_aux)

def  print_top_sentences (dir_in,dir_out,dict_date,rank_sentences,dict_id_tweets,size_sentences,f_out): 
# extract and print top sentences
  num_sentences=0
  for  (count,tweet) in rank_sentences:
    (text,author)=tweet
    author_sin=author[1:]
    id_tweet=dict_id_tweets[text,author]
    if num_sentences > size_sentences:
      break
    f_out.write (('%s\t%s\t%s\t%s\t%s\n') % (dict_date[text,author],author,text,count,id_tweet))
    num_sentences += 1  
  return  
  
  
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# main
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def main():

# intit data

  first_tweet=True
  tweets=0
  num_tweets=0
  max_sentences=1000
  max_sentences_print=1000
  max_sentences_print_day=500
  max_tweet_hour = 15000
  flag_compress=False
  dir_in=''
  dir_out=''
  list_dates_str=[]
  last_line_last_day=0
  time_setting=0
  
  reload(sys)
  sys.setdefaultencoding('utf-8')
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
  start = datetime.datetime.fromtimestamp(time.time())  
  
  parser = argparse.ArgumentParser(description='This script extracts the most spread tweets in global and by day ')
  parser.add_argument('file_in', type=str, help='file with raw tweets')
  parser.add_argument('--dir_in', type=str, default='./', help='Dir data input')
  parser.add_argument('--dir_out', type=str, default='./', help='Dir data output')
  parser.add_argument('--top_size', type=int, default='1000', help='size ranking')
  parser.add_argument('--TZ', type=int, default=0, help='Time zone')
  args = parser.parse_args()

  file_in=args.file_in
  dir_in=args.dir_in
  dir_out=args.dir_out
  max_sentences=args.top_size
  time_setting= args.TZ
  
  filename=re.search (r"([\w-]+)\.([\w\.]*)", file_in)
  print 'file name', file_in
  if not filename:
    print "bad filename",file_in, ' Must be an extension'
    exit (1)
  name=filename.group(0)
  prefix=filename.group(1)
  extension=filename.group(2)
  print extension
  if extension == 'txt.tar.gz':
    flag_compress=True
  try:  
    if flag_compress:
      f_in=gzip.open(dir_in+file_in, 'rb')
      print 'open as compress'
    else:
      f_in = codecs.open(dir_in+file_in, 'rU',encoding='utf-8')
      print 'open as unicode'
  except:
    print 'Can not open file',dir_in+file_in
    exit (1)
 
  sentences= Sentence_similarity(dir_out,prefix,max_sentences)
  sentences_day=Sentence_similarity(dir_out,prefix,max_sentences)
  f_out_global = codecs.open(prefix+'_global_sentences.csv', 'w',encoding='utf-8')
  f_out_day = codecs.open(prefix+'_day_sentences.csv','w',encoding='utf-8')
  f_out_recent = codecs.open(prefix+'_recent_sentences.csv','w',encoding='utf-8')
  for line in f_in:
    if  type(line) is str:
      line=unicode(line, "utf-8")
    tweets= re.findall(r'(\d\d\d\d)-(\d\d)-(\d\d)\s(\d\d):(\d\d):(\d\d)\t(@\w+)\t([^\t\n]+)\tvia=([^\t\n]+)\tid=(\S+)\tfollowers=(\S+)\tfollowing=(\S+)\tstatuses=(\S+)\tloc=([^\t\n]*)',line,re.U)
    if len(tweets) == 0:
       print 'not match '
    else:
      (year,month,day,hour,minutes,seconds, author,text,app,user_id,followers,following,statuses,loc)=tweets[0]
      match=re.match(r'^(\d+)\t',line,re.U)
      if match:
         id_tweet=match.group(1)
      else:
         id_tweet=0
      local_tz=timedelta(hours=time_setting)
      time_GMT= datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minutes), second=int(seconds))
      local_time= time_GMT + local_tz
      year=str(local_time.year)
      month=str(local_time.month)
      day=str(local_time.day)
      hour=str(local_time.hour)
      local_day= datetime.date(year=int(year), month=int(month), day=int(day))
      date=year+'/'+month+'/'+day+' '+hour+':'+minutes+':'+seconds
      current_day=datetime.date(year=int(year), month=int(month), day=int(day))
      (author_source,text_source) = get_tweet_source (text)
      if first_tweet == True:
        start_time= datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minutes), second=int(seconds))
        start_day= current_day
        start_hour=hour
        first_tweet=False
        last_day=current_day
        last_hour=hour
        tweets_hour =0
      tweets_hour= tweets_hour +1
      end_day= datetime.date(year=int(year), month=int(month), day=int(day))
      if tweets_hour > max_tweet_hour:
        print 'day', day, 'hour', hour, 'tweets hour', tweets_hour,'block hour'
        sentences.set_hour()
        sentences_day.set_hour()
        tweets_hour =0
      if hour != last_hour:
        print 'day', day, 'hour', hour, 'tweets hour', tweets_hour
        sentences.set_hour()
        sentences_day.set_hour()
        tweets_hour =0
        last_hour=hour
      #print ' change day',current_day,last_day
      if current_day < last_day: # se evitan dias descolocados de twitter in Id más alto que la fecha
        current_day=last_day
      if current_day > last_day:
        sentences.set_day(last_day)
        sentences_day.set_day(last_day)
        list_texts = sentences_day.get_texts()
        dict_id_tweets= sentences_day.get_id_tweets ()
        dict_sentences_count = sentences_day.get_sentences_count()
        dict_date=sentences_day.get_dict_date()
        num_sentences= len(list_texts)
        print num_sentences, len (dict_sentences_count)
        if num_sentences > max_sentences_print_day:
          num_sentences= max_sentences_print_day
        sentences_rank_order=sorted([(value,key) for (key,value) in dict_sentences_count.items()],reverse=True) 
        day_str= str(last_day)
        list_dates_str.append(day_str)
        print_top_sentences (dir_in,dir_out,dict_date,sentences_rank_order,dict_id_tweets,num_sentences,f_out_day)
        #sentences_day.reset_sentences()
        sentences_day=Sentence_similarity(dir_out,prefix,max_sentences)
        last_day=current_day
 #store similar tweets      
      if last_day==current_day: #ignore tweets not order
        words= token_words_url (text_source)
        set_words= frozenset(words)
        len_set_words=len (set_words)
        if len(words) >=7 and (author != author_source):
          sentences.set_item(words,text_source,author,author_source,date,id_tweet)
          sentences_day.set_item(words,text_source,author,author_source,date,id_tweet)
          #find similarity
     
  #remove sentences not frequent
  sentences.set_hour()
  sentences.set_day(current_day)
  sentences_day.set_day(current_day)
  end_time= datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minutes), second=int(seconds))   
  #finish geting tweets 
  list_texts = sentences_day.get_texts()
  dict_id_tweets= sentences_day.get_id_tweets ()
  dict_sentences_count = sentences_day.get_sentences_count()
  dict_date=sentences_day.get_dict_date()
  num_sentences= len(list_texts)
  print num_sentences, len (dict_sentences_count)
  if num_sentences > max_sentences_print_day:
   num_sentences= max_sentences_print_day
  day_str= str(last_day)
  sentences_rank_order=sorted([(value,key) for (key,value) in dict_sentences_count.items()],reverse=True) 
  print_top_sentences (dir_in,dir_out,dict_date,sentences_rank_order,dict_id_tweets,num_sentences,f_out_recent)
  print_top_sentences (dir_in,dir_out,dict_date,sentences_rank_order,dict_id_tweets,num_sentences,f_out_day)
  
  list_texts = sentences.get_texts()
  dict_id_tweets= sentences.get_id_tweets ()
  dict_sentences_count = sentences.get_sentences_count()
  dict_date=sentences.get_dict_date()
  num_sentences= len(list_texts)
  if num_sentences > max_sentences_print:
    num_sentences=max_sentences_print
  sentences_rank_order=sorted([(value,key) for (key,value) in dict_sentences_count.items()],reverse=True) 
  print_top_sentences (dir_in,dir_out,dict_date, sentences_rank_order,dict_id_tweets,num_sentences,f_out_global)
  #save status
  f_out = codecs.open(dir_out+prefix+'_talk_status.txt', 'w',encoding='utf-8')
  f_out.write(('start_time\t%s\n') % start_time)
  f_out.write(('stop_time\t%s\n') % end_time)
  if flag_compress:
    f_out.write('status\tprocessed\n') 
  else:
    f_out.write('status\tsemiprocesed\n') 
  f_out.write(('last_tweet\t%s\n') % id_tweet)
  f_out.write(('file_length\t%s\n') % f_in.tell())
  f_out.write(('num_tweets\t%s\n') % num_tweets)
  stop = datetime.datetime.fromtimestamp(time.time())
  f_out.write(('runtime\t%s\n') % (stop - start))
  f_in.close()
  f_out.close()
  f_out_global.close()
  f_out_day.close()
  f_out_recent.close()
  exit(0)

if __name__ == '__main__':
  main() 