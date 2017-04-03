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
import unicodedata
import codecs
import gzip
import argparse
  
def filter_tweet (author,text,loc,filter_users,filter_words,filter_names):
  author=author.lower()
  if  author in filter_users:
    return True
  if  author[1:] in filter_users:
    return True
  words=re.findall (r'[@#]*[-\w]+', text,re.U)
  for word in words:
    if word in filter_words:
      return True 
  for name in filter_names:  
    if text.find(name) != -1:
      return True  
  for name in filter_names:  
    if loc.find(name) != -1:
      return True  
  for name in filter_names:  
    if author.find(name) != -1:
      return True  
  return False
  
def select_tweet (author,text,loc,select_users,select_words,select_names):
  author=author.lower()
  if  author in select_users:
    return True
  if  author[1:] in select_users:
    return True
  words=re.findall (r'[@#]*[-\w]+', text,re.U)
  for word in words:
    if word in select_words:
      return True 
  for name in select_names:  
    if text.find(name) != -1:
      return True
  for name in select_names:  
    if loc.find(name) != -1:
      return True
  return False
  
def get_data (file_in):
  dict_data={}
  try:
    f = codecs.open(file_in, 'rU',encoding='utf-8')
  except:
    print 'Can not open file',file_in
    exit (1)  
  for line in f: 
      line= line.strip("\n")
      data=line.split('\t')
      dict_data[(data[0])]=1
  f.close()
  print len(dict_data)
  return dict_data
  
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# main
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def main():
    
  #get parameters
  args = sys.argv[1:]
  filter_users={}
  filter_words={}
  filter_names={}
  select_words={}
  select_names={}
  select_users={}
  filter_days={}
  flag_select=False
  flag_filter=False
  flag_from=False
  flag_to=False
  flag_compress=False
  
  reload(sys)
  sys.setdefaultencoding('utf-8')
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
  parser = argparse.ArgumentParser(description='This script selects or filter tweets by words, sentences, users or dates')
  parser.add_argument('file_in', type=str, help='file with raw tweets')
  parser.add_argument('--select_users', type=str,default='',help='file with user selected (a users by line)')
  parser.add_argument('--select_words', type=str, default='', help='file with words selected (a word by line)')
  parser.add_argument('--select_names', type=str, default='', help='file with sentences selected (a sentence by line)')
  parser.add_argument('--filter_users', type=str, default='', help='file with user filtered (a users by line)')
  parser.add_argument('--filter_words', type=str, default='', help='file with words filtered (a word by line)')
  parser.add_argument('--filter_names', type=str, default='', help='file with sentences filtered (a sentence by line)')
  parser.add_argument('--filter_days', type=str, default='', help='file with days filtered (a date by line)')
  parser.add_argument('--date_from', type=str, default='', help='get tweets for this date')
  parser.add_argument('--date_to', type=str, default='', help='get tweets to this date')
  parser.add_argument('--dir_in', type=str, default='./', help='dir data input')
  parser.add_argument('--dir_out', type=str, default='./', help='dir data output')
  args = parser.parse_args()

  file_in=args.file_in
  if args.select_users != '':
    select_users= get_data(args.select_users)
    flag_select=True
  if args.select_words != '':
    select_words= get_data(args.select_words)
    flag_select=True
  if args.select_names != '':
    select_names= get_data(args.select_names)
    flag_select=True
  if args.filter_users != '':
    filter_users= get_data(args.filter_users)
    flag_filter=True
  if args.filter_words != '':
    filter_words= get_data(args.filter_words)
    flag_filter=True
  if args.filter_names != '':
    filter_names= get_data(args.filter_names)
    flag_filter=True
  if args.filter_days != '':
    filter_days= get_data(args.filter_days)
    flag_filter=True
  if args.date_from !='':
    date = re.search (r'(\d\d\d\d)-(\d\d)-(\d\d)',args.date_from)
    from_day=datetime.datetime(int(date.group(1)), int(date.group(2)),int(date.group(3)))
    flag_from=True
  if args.date_to !='':
    date = re.search (r'(\d\d\d\d)-(\d\d)-(\d\d)',args.date_to)
    to_day=datetime.datetime(int(date.group(1)), int(date.group(2)),int(date.group(3)))
    flag_to=True
  dir_in=args.dir_in
  dir_out=args.dir_out
  filename=re.search (r"([\w-]+)\.([\w\.]*)", file_in)
  if not filename:
    print "bad filename",file_in, ' Must be an extension'
    exit (1)
  name=filename.group(0)
  prefix=filename.group(1)
  extension=filename.group(2)
  # get start time and end time 
  print extension
  if extension == 'txt.tar.gz':
    flag_compress=True
  try:  
    if flag_compress:
      f_in=gzip.open(dir_in+file_in, 'rb')
      file_out=dir_in+prefix+'.sel.gz'
      print 'open as compress'
    else:
      f_in = codecs.open(dir_in+file_in, 'rU',encoding='utf-8')
      file_out=dir_in+prefix+'.sel'
      print 'open as unicode'
  except:
    print 'Can not open file',dir_in+file_in
    exit (1)
  if flag_compress:
    f_out=gzip.open(file_out, 'wb')
  else:
    f_out= codecs.open(file_out,'w',encoding='utf-8')
  print 'reading tweets'  
  num_select_tweets=0
  num_tweets=0
  for line in f_in:
    line_raw=line
    if  type(line) is str:
      line=unicode(line, "utf-8")
    tweets= re.findall(r'(\d\d\d\d)-(\d\d)-(\d\d)\s(\d\d):(\d\d):(\d\d)\t(@\w+)\t([^\t\n]+)\tvia=([^\t\n]+)\tid=(\S+)\tfollowers=(\S+)\tfollowing=(\S+)\tstatuses=(\S+)\tloc=([^\t\n]*)',line,re.U)
    if not tweets:
      print 'Not mach-->'
    else:
      (year,month,day,hour,minutes,seconds, author,text,app,user_id,followers,following,statuses,loc)=tweets[0]
      current_day=datetime.datetime(int(year), int(month),int(day))
      day_str = year +'-'+ month+'-'+ day
      num_tweets=num_tweets +1 
      if num_tweets % 100000 == 0:
        print num_tweets  
      if day_str in filter_days:
        pass
      elif flag_from and  current_day < from_day:   
         pass
      elif flag_to and current_day > to_day: 
         pass
      elif flag_filter:
        if filter_tweet (author,text,loc,filter_users,filter_words,filter_names):
          pass 
        else:  
          f_out.write(line_raw)
      elif flag_select:
        if select_tweet (author,text,loc,select_users,select_words,select_names):
           f_out.write (line_raw)
        else:  
          pass
      else:  
        f_out.write(line_raw)

  f_in.close() 
  f_out.close() 
  exit(0)

if __name__ == '__main__':
  main()

 
