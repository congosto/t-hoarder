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
import codecs
import unicodedata
import gzip
import argparse

  
def strip_accents(s):
   return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

# # A dinamic matrix
# # This matrix is a dict whit only cells it nedeed
# # Column and row numbers start with 1
#  
class AvgDict(dict):
    def __init__(self):
        self._total = 0.0
        self._count = 0

    def __setitem__(self, k, v):
        if k in self:
            self._total -= self[k]
            self._count -= 1
        dict.__setitem__(self, k, v)
        self._total += v
        self._count += 1

    def __delitem__(self, k):
        v = self[k]
        dict.__delitem__(self, k)
        self._total -= v
        self._count -= 1
        
    def store (self,k,v):
        if k not in self:
          self[k]=v
          self._count += 1
        else:
          old_value=self[k]
          self[k]=old_value+v
        self._total += v
        return 
          
    def store_unique (self,k,v):
      if k not in self:
        self[k]=v
        self._count += 1
        self._total = v
        return   
        
    def getitem (self,k):
      if k not in self:
        return 0
      else:
        return self[k]

    def average(self):
        if self._count:
            return self._total/self._count
    
    def total(self):
        total_v =0
        for v in self:
          total_v += v
        return total_v
    
    def reset(self):
        self._total = 0.0
        self._count = 0
        return

class Matrix(dict):
  def __init__(self):
    return
 
  def setitem(self, row, col, v):
      dict.__setitem__(self,(row,col),v)
      return
    
  def getitem(self, row,col):
    if (row,col) not in self:
      return 0
    else:
      return self[(row,col)]  
      
  def store(self, row, col, v):
    if (row,col) not in self:
      dict.__setitem__(self,(row,col),v)
    else:
       old_value=self[(row,col)] 
       dict.__setitem__(self,(row,col),v+old_value)
    return
    
  def store_unique(self, row, col, v):
    if (row,col) not in self:
      dict.__setitem__(self,(row,col),v)
    return
  
class Counters(object):
  def __init__(self,  prefix,d_out,dict_filter,dict_sentences,dict_keywords,top_size):
     self.prefix=prefix
     self.d_out=d_out
     self.dict_filter=dict_filter
     self.dict_sentences=dict_sentences
     self.list_sentences=[]
     self.dict_keywords=dict_keywords
     self.list_keywords=[]
     self.top_size = top_size
     self.count_tweets =0
     self.dict_tweets=AvgDict()
     self.top_authors=[]
     self.dict_apps=AvgDict()
     self.top_apps=[]
     self.dict_locs=AvgDict()
     self.top_locs=[]
     self.dict_words=AvgDict()
     self.top_words=[]
     self.dict_hashtags=AvgDict()
     self.top_hashtags=[]
     self.dict_users_reply=AvgDict()
     self.top_users_reply=[]
     self.dict_users_RT=AvgDict()
     self.top_users_RT=[]
     self.dict_users_mention=AvgDict()
     self.top_users_mention=[]
     self.dict_tweets_day=AvgDict()
     self.dict_RT_day=AvgDict()
     self.dict_reply_day=AvgDict()
     self.dict_mention_day=AvgDict()
     self.dict_authors_unique_day=AvgDict()
     self.dict_authors_new_day=AvgDict()
     self.dict_authors_old_day=AvgDict()
     self.tweets_day_order=[]
     self.dict_authors_day=Matrix()
     self.dict_top_authors_day=Matrix()
     self.dict_top_users_RT_day=Matrix()
     self.dict_top_users_reply_day=Matrix()
     self.dict_top_users_mention_day=Matrix()
     self.dict_top_apps_day=Matrix()
     self.dict_top_locs_day=Matrix()
     self.dict_top_words_day=Matrix()
     self.dict_top_hashtags_day=Matrix()
     self.dict_sentences_day=Matrix()
     self.dict_keywords_day=Matrix()
     return  
       
  def token_words (self,source):
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
    list_tokens=re.findall (r'[#@]*\w+', source_without_urls,re.U) 
    for token in list_tokens:
      if (token.find('#') == -1) and (token.find('@') == -1):
        number= re.search(r'\d+',token)
        if not number:
          token=token.lower()
          list_words.append(token)
    return list_words

  def token_hashtags (self,source):
    source=strip_accents (source)
    list_tokens=re.findall (r'#\w+', source,re.U)
    return list_tokens

  def find_sentences (self,source):
    list_sentences=[]
    for name in self.dict_sentences:
      if source.find(name) != -1:
        list_sentences.append(name)
    return list_sentences
 
  def token_keywords (self,source):
    list_keywords=[]
    list_tokens=re.findall (r'[#@]*[\w-]+', source,re.U)
    for token in list_tokens:
      if   token in self.dict_keywords:
        list_keywords.append(token)
    return list_keywords
    
    
  def write_top (self,top,type_top):
    f_file=codecs.open(self.d_out+self.prefix+'_'+type_top+'.txt', 'w',encoding='utf-8', errors='ignore')
    for item in top:
      f_file.write('%s\n' %(item))
    f_file.close()
    
  ####### methods ################
  
  def set_tweets_day(self,date,text):
    self.count_tweets += 1
    self.dict_tweets_day.store(date,1)
    list_mentions=re.findall (r'@\w+', text)
    if len (list_mentions) >0:
      if re.match(r'[\.]*(@\w+)[^\t\n]+',text):
        self.dict_reply_day.store(date,1)
    
      elif (text.find('rt ') != -1):
        self.dict_RT_day.store(date,1)
      self.dict_mention_day.store(date,1)
    return
 
  def get_tweets_day(self):     
     self.tweets_day_order=sorted([(key,value) for (key,value) in self.dict_tweets_day.items()])
     f_out=  codecs.open(self.d_out+self.prefix+'_tweets_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date,N.Tweets,N.RTs,N.Replies,N.Mentions\n")
     for  (key,value) in self.tweets_day_order:
        f_out.write ('%s,%s,%s,%s,%s\n' % (key,self.dict_tweets_day.getitem(key),self.dict_RT_day.getitem(key),self.dict_reply_day.getitem(key),self.dict_mention_day.getitem(key)))
     f_out.close()
     return
     
  def set_author(self, author):
    self.count_tweets += 1
    self.dict_tweets.store(author,1)
    return
 
  def get_authors(self):
    num_authors=len(self.dict_tweets)
    top_size=self.top_size
    print 'N.tweets %s, N.Authors %s' % (self.count_tweets,num_authors)
    authors_rank_order=sorted([(value,key) for (key,value) in self.dict_tweets.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_authors.txt', 'w',encoding='utf-8') 
    f_out.write ("Author,N.Tweets\n")
    for (value,key) in authors_rank_order:
      f_out.write ('%s,%s\n' % (key,value))
    f_out.close()
    if top_size > num_authors:
      top_size =num_authors
    for i in range (0,top_size):
      (value,author)=authors_rank_order[i]
      self.top_authors.append(author)
    self.write_top (self.top_authors,'top_authors')
    f_out.close()
    return
    
  def set_authors_day(self,date,author):
      if (date,author) not in self.dict_authors_day:
        self.dict_authors_unique_day.store(date,1)
        if author in self.dict_tweets:
          self.dict_authors_old_day.store(date,1)
        else:
          self.dict_authors_new_day.store(date,1)
      if author in self.top_authors:
        index= self.top_authors.index(author)
        self.dict_top_authors_day.store(date,index,1)
      self.dict_tweets.store(author,1)
      self.dict_authors_day.store(date,author,1)
      return
      
  def get_authors_day(self):     
     #write general twittering frecuency 
     f_out=  codecs.open(self.d_out+self.prefix+'_authors_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date,N. Tweets, N. Uniq users,N. New users\n")
     for  (key,value) in self.tweets_day_order:
        f_out.write ('%s,%s,%s,%s\n' % (key,self.dict_tweets_day.getitem(key),self.dict_authors_unique_day.getitem(key),self.dict_authors_new_day.getitem(key)))
     f_out.close()
     #write top authors frecuency 
     f_out=  codecs.open(self.d_out+self.prefix+'_top_authors_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date")
     for author in self.top_authors:
       f_out.write (",%s" % (author))
     top_size=len(self.top_authors)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          f_out.write (',%s' % (self.dict_top_authors_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return

  def set_user_mention(self,text):
    list_mentions=re.findall (r'@\w\w+', text)
    if len (list_mentions) >0:
      user=list_mentions[0]
      if re.match(r'[\.]*(@\w+)[^\t\n]+',text):
        self.dict_users_reply.store(user,1)
      elif (text.find('rt ') != -1):
        self.dict_users_RT.store(user,1)
      for user in list_mentions:
        self.dict_users_mention.store(user,1)
    return
    
  def get_users_reply(self):
    num_users_reply=len(self.dict_users_reply)
    top_size=self.top_size
    users_reply_rank_order=sorted([(value,key) for (key,value) in self.dict_users_reply.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_replies.txt', 'w',encoding='utf-8') 
    f_out.write ("User,N. Replies\n")
    for (value,key) in users_reply_rank_order:
      f_out.write ('%s,%s\n' % (key,value))
    f_out.close()
    if top_size > num_users_reply:
      top_size =num_users_reply
    for i in range (0,top_size):
      (value,user)=users_reply_rank_order[i]
      self.top_users_reply.append(user)
    f_out.close()
    return
    
  def get_users_RT(self):
    num_users_RT=len(self.dict_users_RT)
    top_size=self.top_size
    users_RT_rank_order=sorted([(value,key) for (key,value) in self.dict_users_RT.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_RT.txt', 'w',encoding='utf-8') 
    f_out.write ("User,N. RTs\n")
    for (value,key) in users_RT_rank_order:
      f_out.write ('%s,%s\n' % (key,value))
    f_out.close()
    if top_size > num_users_RT:
      top_size =num_users_RT
    for i in range (0,top_size):
      (value,user)=users_RT_rank_order[i]
      self.top_users_RT.append(user)
    f_out.close()
    return
    
  def get_users_mention(self):
    num_users_mention=len(self.dict_users_mention)
    top_size=self.top_size
    users_mention_rank_order=sorted([(value,key) for (key,value) in self.dict_users_mention.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_mentions.txt', 'w',encoding='utf-8') 
    f_out.write ("User,N. Mentions\n")
    for (value,key) in users_mention_rank_order:
      f_out.write ('%s,%s\n' % (key,value))
    f_out.close()
    if top_size > num_users_mention:
      top_size =num_users_mention
    for i in range (0,top_size):
      (value,user)=users_mention_rank_order[i]
      self.top_users_mention.append(user)
    self.write_top (self.top_users_mention,'top_mentions')
    f_out.close()
    return
    
  def set_user_mention_day(self,date,text):
    list_mentions=re.findall (r'@\w+', text)
    if len (list_mentions) >0:
      user=list_mentions[0]
      if re.match(r'[\.]*(@\w+)[^\t\n]+',text):
        if user in self.top_users_reply:
          index= self.top_users_reply.index(user)
          self.dict_top_users_reply_day.store(date,index,1)
      elif (text.find('rt ') != -1):
        if user in self.top_users_RT:
          index= self.top_users_RT.index(user)
          self.dict_top_users_RT_day.store(date,index,1)
      for user in list_mentions:
        if user in self.top_users_mention:
          index= self.top_users_mention.index(user)
          self.dict_top_users_mention_day.store(date,index,1)
    return
    
  def get_users_reply_day(self):     
     #write general twittering frecuency 
     f_out=  codecs.open(self.d_out+self.prefix+'_top_reply_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date")
     for user in self.top_users_reply:
       f_out.write (",%s" % (user))
     top_size=len(self.top_users_reply)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          f_out.write (',%s' % (self.dict_top_users_reply_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return  
     
  def get_users_RT_day(self):     
     #write general twittering frecuency 
     f_out=  codecs.open(self.d_out+self.prefix+'_top_RT_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date")
     for user in self.top_users_RT:
       f_out.write (",%s" % (user))
     top_size=len(self.top_users_RT)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          f_out.write (',%s' % (self.dict_top_users_RT_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return 
     
  def get_users_mention_day(self):     
     #write general twittering frecuency 
     f_out=  codecs.open(self.d_out+self.prefix+'_top_mention_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date")
     for user in self.top_users_mention:
       f_out.write (",%s" % (user))
     top_size=len(self.top_users_mention)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          f_out.write (',%s' % (self.dict_top_users_mention_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return

  def set_app(self, app):
    self.dict_apps.store(app,1)
    return
    
  def get_apps(self):
    num_apps=len(self.dict_apps)
    top_size=self.top_size
    print 'Num Apps %s' % (num_apps)
    apps_rank_order=sorted([(value,key) for (key,value) in self.dict_apps.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_apps.txt', 'w',encoding='utf-8') 
    f_out.write ("App,N. Tweets\n")
    for (value,key) in apps_rank_order:
      f_out.write ('%s,%s\n' % (key,value))
    f_out.close()
    if top_size > num_apps:
      top_size =num_apps
    for i in range (0,top_size):
      (value,app)=apps_rank_order[i]
      self.top_apps.append(app)
    self.write_top (self.top_apps,'top_apps')  
    f_out.close()
    return
    
  def set_apps_day(self,date,app):
    if app in self.top_apps:
       index= self.top_apps.index(app)
       self.dict_top_apps_day.store(date,index,1)
    return
      
  def get_apps_day(self):     
     f_out=  codecs.open(self.d_out+self.prefix+'_top_apps_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date")
     for app in self.top_apps:
       f_out.write (",%s" % (app))
     top_size=len(self.top_apps)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          f_out.write (',%s' % (self.dict_top_apps_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return
      
  def set_loc(self, loc):
      if (loc=='none') or (len(loc)==0):
        self.dict_locs.store('desconocida',1)
      else:
        self.dict_locs.store(loc,1)
      return
    
  def get_locs(self):
    num_locs=len(self.dict_locs)
    top_size=self.top_size
    print 'Num Locs %s' % (num_locs)
    locs_rank_order=sorted([(value,key) for (key,value) in self.dict_locs.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_locs.txt', 'w',encoding='utf-8') 
    f_out.write ("Loc,N. Tweets\n")
    for (value,key) in locs_rank_order:
      f_out.write ('%s,%s\n' % (key,value))
    f_out.close()
    if top_size > num_locs:
      top_size =num_locs
    for i in range (0,top_size):
      (value,loc)=locs_rank_order[i]
      self.top_locs.append(loc)
    f_out.close()
    return  
    
  def set_locs_day(self,date,loc):
      if loc in self.top_locs:
        index= self.top_locs.index(loc)
        self.dict_top_locs_day.store(date,index,1)
      return  
      
  def get_locs_day(self):     
     locs_day_matrix_order=sorted([(key,value) for (key,value) in self.dict_locs_day.items()])
     f_out=  codecs.open(self.d_out+self.prefix+'_locs_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date,Loc,Count\n")
     for  (key,value) in locs_day_matrix_order:
        (date,loc)=key
        f_out.write ('%s,%s,%s,%s,%s\n' % (loc,self.dict_locs_day[key]))
     f_out.close()
     return
      
  def set_words(self, text):
    words=self.token_words(text)
    for word in words:
      if word not in self.dict_filter:
         self.dict_words.store(word,1)
    return
    
  def get_words(self):
    num_words=len(self.dict_words)
    top_size=self.top_size
    print 'Num words %s' % (num_words)
    words_rank_order=sorted([(value,key) for (key,value) in self.dict_words.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_words.txt', 'w',encoding='utf-8') 
    f_out.write ("Word,Count\n")
    for (value,key) in words_rank_order:
      f_out.write ('%s,%s\n' % (key,value))
    f_out.close()
    if top_size > num_words:
      top_size =num_words
    for i in range (0,top_size):
      (value,word)=words_rank_order[i]
      self.top_words.append(word)
    self.write_top (self.top_words,'top_words')
    return  

  def set_words_day(self,date,text):
    words=self.token_words(text)
    for word in words:
      if word in self.top_words:
          index= self.top_words.index(word)
          self.dict_top_words_day.store(date,index,1)
    return  
      
  def get_words_day(self):     
     f_out=  codecs.open(self.d_out+self.prefix+'_top_words_day.txt', 'w',encoding='utf-8') 
     f_out.write ("date")
     for word in self.top_words:
       f_out.write (",%s" % (word))
     top_size=len(self.top_words)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          #print date,i
          f_out.write (',%s' % (self.dict_top_words_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return
      
  def set_hashtags(self, text):
    words=self.token_hashtags(text)
    for word in words:
       self.dict_hashtags.store(word,1)
    return
    
  def get_hashtags(self):
    num_hashtags=len(self.dict_hashtags)
    top_size=self.top_size
    print 'Num Hashtags %s' % (num_hashtags)
    hashtags_rank_order=sorted([(value,key) for (key,value) in self.dict_hashtags.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_hashtags.txt', 'w',encoding='utf-8') 
    f_out.write ("Hashtag,Count\n")
    for (value,hashtag) in hashtags_rank_order:
      f_out.write ('%s,%s\n' % (hashtag,value))
    f_out.close()
    if top_size > num_hashtags:
      top_size =num_hashtags
    for i in range (0,top_size):
      (value,hashtag)=hashtags_rank_order[i]
      self.top_hashtags.append(hashtag)
    self.write_top (self.top_hashtags,'top_hashtags')    
    return  
   
  def set_hashtags_day(self,date,text):
    words=self.token_hashtags(text)
    for word in words:
      if word in self.top_hashtags:
          index= self.top_hashtags.index(word)
          self.dict_top_hashtags_day.store(date,index,1)
    return  
    
  def get_hashtags_day(self):
     f_out=  codecs.open(self.d_out+self.prefix+'_top_hashtags_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date")
     for hashtag in self.top_hashtags:
       f_out.write (",%s" % (hashtag))
     top_size=len(self.top_hashtags)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          #print date,i
          f_out.write (',%s' % (self.dict_top_hashtags_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return  
    
  def set_sentences(self, text):
    sentences=self.find_sentences(text)
    for name in sentences:
      self.dict_sentences.store(name,1)
    return
       
  def get_sentences(self):
    num_sentences=len(self.dict_sentences)
    top_size=self.top_size
    print 'Num Sentences %s' % (num_sentences)
    sentences_rank_order=sorted([(value,key) for (key,value) in self.dict_sentences.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_sentences.txt', 'w',encoding='utf-8') 
    for (value,key) in sentences_rank_order:
      f_out.write ('%s\n' % (key))
      self.list_sentences.append(key)
    f_out.close()
    return  
    
  def set_sentences_day(self,date,text):
    sentences=self.find_sentences(text)
    for name in sentences:
       index= self.list_sentences.index(name)
       self.dict_sentences_day.store(date,index,1)
    return  
    
  def get_sentences_day(self):
     f_out=  codecs.open(self.d_out+self.prefix+'_sentences_day.txt', 'w',encoding='utf-8') 
     f_out.write ("date")
     for name in self.list_sentences:
       f_out.write (",%s" % (name))
     top_size=len(self.list_sentences)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          #print date,i
          f_out.write (',%s' % (self.dict_sentences_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close() 
      
  def set_keywords(self, text):
    words=self.token_keywords(text)
    for word in words:
      self.dict_keywords.store(word,1)
    return
  
  def get_keywords(self):
    num_keywords=len(self.dict_keywords)
    top_size=self.top_size
    print 'Num Keywords %s' % (num_keywords)
    keywords_rank_order=sorted([(value,key) for (key,value) in self.dict_keywords.items()],reverse=True)
    f_out=  codecs.open(self.d_out+self.prefix+'_keywords.txt', 'w',encoding='utf-8') 
    for (value,key) in keywords_rank_order:
      f_out.write ('%s\n' % (key))
      self.list_keywords.append(key)
    f_out.close()
    return  
    
  def set_keywords_day(self,date,text):
    words=self.token_keywords(text)
    for word in words:
       index= self.list_keywords.index(word)
       self.dict_keywords_day.store(date,index,1)
    return  

  def get_keywords_day(self):
     f_out=  codecs.open(self.d_out+self.prefix+'_keywords_day.txt', 'w',encoding='utf-8') 
     f_out.write ("Date")
     for keyword in self.list_keywords:
       f_out.write (",%s" % (keyword))
     top_size=len(self.list_keywords)
     for (date,value) in self.tweets_day_order:
       f_out.write ('\n%s' % (date))
       for i in range (0,top_size):
          f_out.write (',%s' % (self.dict_keywords_day.getitem(date,i)))
     f_out.write ( "\n")
     f_out.close()
     return 
      
  def set_keywords(self, text):
    words=self.token_keywords(text)
    for word in words:
      self.dict_keywords.store(word,1)
    return

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# main
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def main():

# intit data
  filter_words={}
  flag_compress=False
  flag_sentences=False
  flag_keywords=False
  flag_filter=False
  dict_filter=AvgDict()
  dict_sentences=AvgDict()
  dict_keywords=AvgDict()
  first_tweet=True
  num_tweets=0

  reload(sys)
  sys.setdefaultencoding('utf-8')
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
  start = datetime.datetime.fromtimestamp(time.time())
    
  parser = argparse.ArgumentParser(description='This script extracts the content of a set of tweets for grouping them in days, additionally it takes note of authors, hashtag, words and specific sentences in each day.')
  parser.add_argument('file_in', type=str, help='file with raw tweets')
  parser.add_argument('--dir_in', type=str, default='./', help='Dir data input')
  parser.add_argument('--dir_out', type=str, default='./', help='Dir data output')
  parser.add_argument('--top_size', type=int, default='100', help='size ranking')
  parser.add_argument('--filter', type=str, default='', help='file with stop words for filtering')
  parser.add_argument('--sentences', type=str, default='', help='file sentences to analyze')
  parser.add_argument('--keywords', type=str, default='', help='file keywords to analyze')
  parser.add_argument('--TZ', type=int, default=0, help='Time zone')
  args = parser.parse_args()

  file_in=args.file_in
  dir_in=args.dir_in
  dir_out=args.dir_out
  top_size=args.top_size
  sentences_file=args.sentences
  keywords_file=args.keywords
  time_setting= args.TZ

  flag_old= False
  if sentences_file != '':
    flag_sentences=True
  if keywords_file != '':
    flag_keywords=True
    
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
  # get start time and end time 
  if  flag_filter:
    f = codecs.open(dir_in+filter_words_file, 'rU',encoding='utf-8')
    list_words= re.findall(r'\w+',f.read(),re.U)
    for word in list_words:
      dict_filter[word]=1
    f.close()
  if  flag_sentences: 
    f = codecs.open(dir_in+sentences_file, 'rU',encoding='utf-8')
    for line in f: 
      item=line.strip("\n")
      dict_sentences.store_unique(item.lower(),1)
    f.close()
  if flag_keywords:      
    f = codecs.open(dir_in+keywords_file, 'rU',encoding='utf-8')
    list_keywords= re.findall(r'[\S]+',f.read(),re.U)
    for word in list_keywords:
       dict_keywords.store_unique(word.lower(),1)
    f.close()
  try:  
    if flag_compress:
      f_in=gzip.open(dir_in+file_in, 'rb')
      top_size=1000
      print 'open as compress'
    else:
      f_in = codecs.open(dir_in+file_in, 'rU',encoding='utf-8')
      print 'open as unicode'
  except:
    print 'Can not open file',dir_in+file_in
    exit (1)
  counters=Counters(prefix,dir_out,dict_filter,dict_sentences,dict_keywords,top_size)
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
       text=text.lower()
       author=author.lower()
       loc=loc.lower()
       num_tweets=num_tweets +1 
       if num_tweets % 10000 == 0:
          print num_tweets  
       if first_tweet == True:
          start_time= datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minutes), second=int(seconds))
          start_day= datetime.date(year=int(year), month=int(month), day=int(day))
          first_tweet=False
       counters.set_author(author)
       counters.set_user_mention(text)
       counters.set_app(app)
       counters.set_loc(loc)
       counters.set_words(text)
       counters.set_hashtags(text)
       if flag_sentences:
         counters.set_sentences(text)
       if flag_keywords:
         counters.set_keywords(text)
 #end loop first pass
  end_time= datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minutes), second=int(seconds))
  end_day= datetime.date(year=int(year), month=int(month), day=int(day))
  end_hour=int(hour)
  duration=end_time-start_time
  num_days=int(duration.days)
  print  'Start time',start_time,'End time',end_time,'Total duration',duration,'days', num_days
  counters.get_authors()
  counters.get_users_RT()
  counters.get_users_reply()
  counters.get_users_mention()
  counters.get_apps()
  counters.get_locs()
  counters.get_words()
  counters.get_hashtags()
  if flag_sentences:
    counters.get_sentences()
  if flag_keywords:
    counters.get_keywords()
  print "second pass"
  num_tweets=0
  first_tweet = True
  f_in.seek(0) 
# Group tweets in days and get information
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
      year=local_time.year
      month=local_time.month
      day=local_time.day
      hour=local_time.hour
      local_day= datetime.date(year=int(year), month=int(month), day=int(day))
      num_tweets=num_tweets +1 
      if num_tweets % 10000 == 0:
        print num_tweets  
      if first_tweet == True:
         start_time= local_time
         start_day= local_day
         last_day= start_day
         last_hour= hour
         first_tweet=False
      current_time= local_time
      current_day= local_day
      if current_day != last_day:
          print 'current day',current_day
          last_day=current_day
      author=author.lower()
      text=text.lower()
      loc=loc.lower()
      date=current_day.strftime('%Y%m%d')
      counters.set_tweets_day(date,text)
      counters.set_authors_day(date,author)
      counters.set_user_mention_day(date,text)
      counters.set_apps_day(date,app)
      counters.set_locs_day(date,loc)
      counters.set_words_day(date,text)
      counters.set_hashtags_day(date,text)
      if flag_sentences:
        counters.set_sentences_day(date,text)
      if flag_keywords:
        counters.set_keywords_day(date,text)
  #end loop second pass
  counters.get_tweets_day()
  counters.get_authors_day()
  counters.get_users_RT_day()
  counters.get_users_reply_day()
  counters.get_users_mention_day()
  counters.get_apps_day()
  counters.get_words_day()
  counters.get_hashtags_day()
  if flag_sentences:
    counters.get_sentences_day()
  if flag_keywords:
    counters.get_keywords_day()
    #save status
  f_out = codecs.open(dir_out+prefix+'_counter_status.txt', 'w',encoding='utf-8')
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
if __name__ == '__main__':
  main()
