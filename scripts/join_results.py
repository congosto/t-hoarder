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
import codecs
import argparse
import commands

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
        
class JoinCounters(object):
  def __init__(self,  experiment,dir_in,dir_out,top):
    self.experiment=experiment
    self.dir_in= dir_in
    self.dir_out= dir_out
    self.top= top
    self.top_counters=AvgDict()
    self.top_entities_day=AvgDict()
    self.top_entities=AvgDict()
    self.dates=AvgDict()
    self.entities_day={}
    self.top_talk=AvgDict()
    self.talk_day={}
    return
    
  def reset_context(self):
    self.top_counters.clear()
    self.top_entities_day.clear()
    self.top_entities.clear()
    self.dates.clear()
    self.entities_day.clear()
    return

  def counters_top(self,pack,file_counters_top):
    file_in='%s/streaming_%s_%s_%s' % (self.dir_in,self.experiment,pack,file_counters_top)
    #print '---> open file',file_in
    try:
      f_in=codecs.open(file_in, 'rU',encoding='utf-8')
    except:
      print 'can not open',file_in
      return
    num_line=0
    for line in f_in:
      if num_line == 0:
        line=line.strip('\n')
        top_entities=line.split(',')
      else:
        line=line.strip('\n')
        data=line.split(',')
        len_data=len(data)
        for i in range (1,len_data):
          try:
            self.top_entities_day.store((data[0],top_entities[i]),int(data[i]))
            self.top_entities.store(top_entities[i],int(data[i]))
            self.dates.store_unique(data[0],1)
          except:
            pass     
      num_line +=1
    f_in.close()
    return
    
      
  def get_counters_top (self,file_counters_top):
    top_ten=[]
    file_out='%s/%s_%s' % (self.dir_out,self.experiment,file_counters_top)
    counters_top_order=sorted([(value,key) for (key,value) in self.top_entities.items()],reverse=True)
    f_out=codecs.open(file_out,'w',encoding='utf-8')
    i=0
    f_out.write('date')
    for (value,key) in counters_top_order:
      #print key,value
      top_ten.append(key)
      f_out.write(',%s' %(key))
      if i > self.top:
        break
      i += 1
    #print top_ten
    dates_order=sorted([(key,value) for (key,value) in self.dates.items()])
    last_date=''
    for (date,value) in dates_order:
      f_out.write('%s' % (date))
      for entity in top_ten:
        f_out.write(',%s' % (self.top_entities_day.getitem((date,entity))))
      f_out.write('\n')  
    f_out.close()
    return
    
  def counters_date(self,pack,file_counters_date):
    file_in='%s/streaming_%s_%s_%s' % (self.dir_in,self.experiment,pack,file_counters_date)
    #print '---> open file',file_in
    try:
      f_in=codecs.open(file_in, 'rU',encoding='utf-8')
    except:
      print 'can not open',file_in
      return
    num_line=0
    for line in f_in:
      if num_line == 0:
        self.head=line
      else:
        line=line.strip('\n')
        data=line.split(',')
        len_data=len(data)
        values=data[1:]
        if data[0] not in self.entities_day:
          self.entities_day[data[0]]=values
        else:
          print 'data duplicate'
          values_ant = self.entities_day[data[0]]
          for i in range (0,len(values)):
            values[i]=str(int(values[i])+int(values_ant[i]))
          self.entities_day[data[0]]=values
      num_line +=1
    f_in.close()
    return
     
  def get_counters_date (self,file_counters_data):
    file_out='%s/%s_%s' % (self.dir_out,self.experiment,file_counters_data)
    counters_data_order=sorted([(key,values) for (key,values) in self.entities_day.items()])
    f_out=codecs.open(file_out,'w',encoding='utf-8')
    f_out.write(self.head)
    for (key,values) in counters_data_order:
      f_out.write('%s' % (key))
      for i in range (0,len(values)):
        f_out.write(',%s' % (values[i]))
      f_out.write('%\n')
    f_out.close()
    return
    
  def talk_top(self,pack,file_talk_top):
    file_in='%s/streaming_%s_%s_%s' % (self.dir_in,self.experiment,pack,file_talk_top)
    #print '---> open file',file_in
    try:
      f_in=codecs.open(file_in, 'rU',encoding='utf-8')
    except:
      print 'can not open',file_in
      return
    for line in f_in:
      line=line.strip('\n')
      data=line.split('\t')
      len_data=len(data)
      try:
       key=(data[0],data[1],data[2],data[4])
       value=int(data[3])
       self.top_talk.store(key,value)
      except:
       pass
    f_in.close()
    return
     
  def get_talk_top (self,file_talk_top):
    file_out='%s/%s_%s' % (self.dir_out,self.experiment,file_talk_top)
    counters_top_talk_order=sorted([(value,key) for (key,value) in self.top_talk.items()],reverse=True)
    f_out=codecs.open(file_out,'w',encoding='utf-8')
    i=0
    for (value,key) in counters_top_talk_order:
      (date,author,text,id_tweet)=key
      f_out.write('%s\t%s\t%s\t%s\t%s\n' % (date,author,text,value,id_tweet))
      if i > 1000:
        break
    f_out.close()
    return
    

def main():
    
  #get parameters
  files_counters_top=['top_authors_day.txt','top_RT_day.txt','top_reply_day.txt','top_mention_day.txt',
            'top_apps_day.txt','top_words_day.txt','top_hashtags_day.txt']
  #files_counters_top=['top_authors_day.txt','top_RT_day.txt','top_reply_day.txt','top_mention_day.txt',
  #          'top_words_day.txt','top_hashtags_day.txt']
  files_counters_date=['tweets_day.txt','authors_day.txt']
  files_talk_top=['global_sentences.csv']
  files_talk_date=['day_sentences.csv']
  files_loc_cat=['location_day.csv','geolocation_day.csv']
  

  parser = argparse.ArgumentParser(description='This script join resuls from blocks processed')
  parser.add_argument('experiment', type=str, help='experiment name')
  parser.add_argument('--dir_in', type=str, default='./', help='Dir data input')
  parser.add_argument('--dir_out', type=str, default='./', help='Dir data output')
  parser.add_argument('--top', type=int, default='10', help='size ranking')
  args = parser.parse_args()

  dir_in=args.dir_in
  dir_out=args.dir_out
  experiment=args.experiment
  top=args.top
  file_log='%s/%s_join_status.txt' % (dir_out,experiment)
  f_log=codecs.open(file_log,'w',encoding='utf-8')
  num_pack=0
  # joining counters
  joining=JoinCounters(experiment,dir_in,dir_out,top)
  start = datetime.datetime.fromtimestamp(time.time())
  print 'processing counters file_counters_top'
  start_file_counters_top=datetime.datetime.fromtimestamp(time.time())
  for file_counters_top in  files_counters_top:
    joining.reset_context()
    while True:
      pack='%s/streaming_%s_%s_%s' % (dir_in,experiment,num_pack,file_counters_top)
      (status, output) =commands.getstatusoutput('ls '+pack)
      #print status, output
      if status !=0:
        break
      joining.counters_top(num_pack,file_counters_top)
      print 'processed', pack
      num_pack += 1
    joining.get_counters_top(file_counters_top)
    num_pack=0
  stop = datetime.datetime.fromtimestamp(time.time())
  f_log.write(('counters_top runtime\t%s\n') % (stop - start_file_counters_top))
  print 'processing counters file_counters_date'
  start_file_counters_date=datetime.datetime.fromtimestamp(time.time())
  for file_counters_date in  files_counters_date:
    joining.reset_context()
    while True:
      pack='%s/streaming_%s_%s_%s' % (dir_in,experiment,num_pack,file_counters_date)
      (status, output) =commands.getstatusoutput('ls '+pack)
      #print status, output
      if status !=0:
        break
      joining.counters_date(num_pack,file_counters_date)
      print 'processed', pack
      num_pack += 1
    joining.get_counters_date(file_counters_date)
    num_pack=0
  stop = datetime.datetime.fromtimestamp(time.time())
  f_log.write(('counters_date runtime\t%s\n') % (stop - start_file_counters_date))
  print 'processing talk_top'
  start_file_talk_top=datetime.datetime.fromtimestamp(time.time())
  for file_talk_top in  files_talk_top:
    joining.reset_context()
    while True:
      pack='%s/streaming_%s_%s_%s' % (dir_in,experiment,num_pack,file_talk_top)
      (status, output) =commands.getstatusoutput('ls '+pack)
      #print status, output
      if status !=0:
        break
      joining.talk_top(num_pack,file_talk_top)
      print 'processed', pack
      num_pack += 1
    joining.get_talk_top(file_talk_top)
    num_pack=0
  stop = datetime.datetime.fromtimestamp(time.time())
  f_log.write(('file_talk_top runtime\t%s\n') % (stop - start_file_talk_top))
  print 'processing loc_cat'
  start_file_loc_cat=datetime.datetime.fromtimestamp(time.time())
  for file_loc_cat in  files_loc_cat:
    file_out='%s/%s_%s' % (dir_out,experiment,file_loc_cat)
    print 'borrando',file_out
    (status, output) =commands.getstatusoutput('rm '+ file_out)
    while True:
      pack='%s/streaming_%s_%s_%s' % (dir_in,experiment,num_pack,file_loc_cat)
      (status, output) =commands.getstatusoutput('ls '+pack)
      #print status, output
      if status !=0:
        break
      print   'cat '+pack+ ' >>' + file_out
      (status, output) =commands.getstatusoutput('cat '+pack+ ' >>' + file_out)
      num_pack +=1
    num_pack=0
  stop = datetime.datetime.fromtimestamp(time.time())
  f_log.write(('file_loc_cat runtime\t%s\n') % (stop - start_file_loc_cat))
  stop = datetime.datetime.fromtimestamp(time.time())
  f_log.write(('total runtime\t%s\n') % (stop - start))
  f_log.close()
  exit(0)

if __name__ == '__main__':
  main()

 