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

class Location (object):
  def __init__(self,  prefix,file_regions, file_areas,file_locations):
    self.prefix=prefix

    self.dict_regions={}
    self.dict_areas={}
    self.list_areas=[]
    self.dict_locations={}
    self.list_locations=[]
    self.num_users = 0
    self.num_users_spain=0
    self.num_regions=0
    self.num_areas=0
    self.num_locations=0
    self.num_users_region=0
    self.num_users_area=0
    self.num_users_loc=0
    self.num_users_unknow=0
    self.list_stopwords= [u'la',u'el',u'las',u'los',u'de',u'del']
    self.list_spain_names=[u'españa',u'spain',u'espagna', u'espana',u'spanien',u'spagna',u'espanha',u'espana']
    self.stopwords= frozenset(self.list_stopwords)
        
    try:  
      f_in = codecs.open(file_regions, 'rU',encoding='utf-8')
    except:
      print 'Can not open file regions ',file_regions
      exit (1)
    num_line=0  
    for line in f_in:
      num_line += 1
      if num_line > 1:
        data = line.split(";")
        num_region=int(data[0])
        name_region=data[1]
        name_region=name_region.lower()
        names=name_region.split("/")
        latitude=data[2]
        longitude=data[3].strip("\n")
        self.dict_regions[num_region]= (names,latitude,longitude)
        self.num_regions +=1
        #if len(names)> 1:
          #print names
    #  print self.dict_regions
    f_in.close()
    
    try:  
      f_in = codecs.open(file_areas, 'rU',encoding='utf-8')
    except:
      print 'Can not open file areas ',file_areas
      exit (1)
    num_line=0  
    for line in f_in:
      num_line += 1
      if num_line > 1:
        data = line.split(";")
        num_region=int(data[0])
        num_area=int(data[1])
        name_area=data[2]
        name_area=name_area.lower()
        names=name_area.split("/")
        latitude=data[3]
        longitude=data[4].strip("\n")
        self.dict_areas[(num_region,num_area)]= (names,latitude,longitude) 
        self.list_areas.append(names[0])
        self.num_areas +=1
        #if len(names)> 1:
          #print num_region,names
    f_in.close()
    
    try:  
      f_in = codecs.open(file_locations, 'rU',encoding='utf-8')
    except:
      print 'Can not open file locations ',file_locations
      exit (1)
    num_line=0  
    for line in f_in:
      num_line += 1
      if num_line > 1:
        data = line.split(";")
        num_area=int(data[0])
        num_loc= int(data[1])
        name_loc=data[2]
        name_loc=name_loc.lower()
        names=name_loc.split("/")
        first_name=names[0]  # is the name usinf for normalizing
        latitude=data[3]
        longitude=data[4].strip("\n")
        for name in names:
          name_aux= strip_accents(name)
          words=name_aux.split(" ")
          if words[0] in self.list_stopwords:
            first_word=words[1]
          else:
            first_word=words[0]
          set_words= frozenset(words)
          set_words = set_words - self.stopwords
          if first_word in self.dict_locations:
            list_names= self.dict_locations[first_word]
          else:
            list_names=[]
          data_location=(num_area,num_loc,set_words,first_name,latitude,longitude)
          list_names.append(data_location)
          self.dict_locations[first_word]= list_names
  
    #print self.list_locations
    f_in.close()
    return
    
  def is_spain (self,text_location):
    found_spain='NA'
    for spain_name in self.list_spain_names:
      if spain_name in text_location:
        #found region
          #print spain_name,text_location
          found_spain=self.list_spain_names[0]
          #print 'found region',found_region,'->', text_location
          self.num_users_spain +=1
          return (found_spain,0,0)
    return ('NA',0,0)   
 
  def get_region (self,text_location):
    found_region='NA'
    for (num_region) in self.dict_regions:
      (region_names,latitude,longitude)=self.dict_regions[num_region]
      for region_name in region_names:
        region_name=strip_accents(region_name)
        if region_name in text_location:
          #found region 
          found_region=region_names[0]
          #print 'found region',region_name,'->', text_location
          self.num_users_region +=1
          return (found_region,latitude,longitude)
    return (found_region,0,0)       
   
  def get_area(self,text_location): 
    found_area='NA'
    for (num_region,num_area) in self.dict_areas:
      (area_names,latitude,longitude)= self.dict_areas[num_region,num_area]
      for area_name in area_names:
        area_name=strip_accents(area_name)
        if area_name in text_location:
        #found area 
          found_area=area_names[0]
          (found_region,latitude,longitude)=self.dict_regions[num_region]
          #print 'found area', found_area,'->', text_location
          self.num_users_area +=1
          return (found_area,latitude,longitude)
    return (found_area,0,0)
   
  def get_location (self,text_location):
    text_location=text_location.lower()
    text_location_aux=strip_accents(text_location)
    (found_spain,found_latitude,found_longitude)=self.is_spain (text_location)
    (found_region,found_latitude,found_longitude)= self.get_region (text_location_aux)
    (found_area,found_latitude,found_longitude)= self.get_area(text_location_aux) 
    found_location='NA'
    self.num_users +=1
    if self.num_users %1000 ==0:
      print self.num_users
    text_location_aux=re.sub('[,;.-/\t\n\r]+', ' ', text_location_aux)
    text_location_aux=re.sub('sta.', 'santa', text_location_aux)
    words=text_location_aux.split(" ")
    set_words= frozenset(words)
    set_words= set_words - self.stopwords
    for word in words:
      if word in  self.dict_locations:
        list_names=self.dict_locations[word]
        most_similarity=0
        for (num_area,num_loc,set_words_loc,first_name,latitude,longitude) in list_names:
          max_similarity = len (set_words_loc)
          similarity= len (set_words & set_words_loc)
          if (similarity == max_similarity):
            found_location= first_name
            break
        if found_location != 'NA':
          self.num_users_loc +=1
          found_latitude=latitude
          found_longitude=longitude
          for (num_region,i) in self.dict_areas:
            if i == num_area:
              (names_region,latitude,longitude)= self.dict_regions[num_region]
              found_region= names_region[0]
              (names_area,latitude,longitude)=self.dict_areas[num_region,num_area]
              found_area= names_area[0]
              found_spain=self.list_spain_names[0]
              return ( found_spain,found_region,found_area,found_location,found_latitude,found_longitude)
    if found_spain=='NA':
      if  (found_region=='NA') and (found_area=='NA') and (found_location=='NA'):
        self.num_users_unknow +=1
      else:
        found_spain=self.list_spain_names[0]
    latitude=0
    longitude=0    
    #print 'no found', text_location
    return ( found_spain,found_region,found_area,found_location,found_latitude,found_longitude)

  def get_location_geocode (latitude,longitude):
    return
    
  def get_statistics(self):
    per_spain = (self.num_users_spain *100.0)/self.num_users
    per_region = (self.num_users_region *100.0)/self.num_users
    per_area = (self.num_users_area *100.0)/self.num_users
    per_location = (self.num_users_loc *100.0)/self.num_users
    per_unknow = ((self.num_users_unknow ) *100.0)/self.num_users
    statistics= (('Percentage españa = %.2f\nPercentage regions = %.2f\nPercentage areas = %.2f\nPercentage location = %.2f\nPercentage unknow = %.2f\n') %  (per_spain,per_region,per_area,per_location,per_unknow)) 
    print statistics  
    return (per_location,statistics)
  def get_geocode (text):
    return
  
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# main
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def main():

# intit data
  #get parameters
  args = sys.argv[1:]
  first_tweet=True
  flag_compress=False
  nun_tweets_geoloc=0
  dir_out=''
  dir_in=''
  time_setting=0

  reload(sys)
  sys.setdefaultencoding('utf-8')
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
  start = datetime.datetime.fromtimestamp(time.time())
  
  parser = argparse.ArgumentParser(description='This script extracts the location data from tweets ')
  parser.add_argument('file_in', type=str, help='file with raw tweets')
  parser.add_argument('resources', type=str, help='file with Spanish location information')
  parser.add_argument('--dir_in', type=str, default='./', help='Dir data input')
  parser.add_argument('--dir_out', type=str, default='./', help='Dir data output')
  parser.add_argument('--TZ', type=int, default=0, help='Time zone')
  args = parser.parse_args()

  file_in=args.file_in
  file_resources=args.resources
  dir_in=args.dir_in
  dir_out=args.dir_out
  time_setting= args.TZ
  filename=re.search (r"([\w-]+)\.([\w\.]*)", file_in)
  print 'file name', file_in
  if not filename:
    print "bad filename",file_in, ' Must be an extension'
    exit (1)
  prefix=filename.group(1)
  extension=filename.group(2)
  print extension
  if extension == 'txt.tar.gz':
    flag_compress=True
  try:  
    f_in = codecs.open(dir_in+file_in, 'rU',encoding='utf-8')
  except:
    print 'Can not open file in ',dir_in+file_in
    exit (1)
  try:  
    if flag_compress:
      f_in=gzip.open(dir_in+file_in, 'rb')
      print 'open as compress'
    else:
      f_in = codecs.open(dir_in+file_in, 'rU',encoding='utf-8')
      print 'open as unicode'
  except:
    print 'Can not open file',dir_in+file_in
  f_location = codecs.open(dir_out+prefix+'_location.txt', 'w',encoding='utf-8')
  try:  
    f_resources = codecs.open(file_resources, 'rU',encoding='utf-8')
  except:
    print 'Can not open file resources ',file_resources
    exit (1)
  for line in f_resources:
    data = line.split(";")
    resource=data[0]
    file_resource=data[1].strip("\n")
    print resource,file_resource
    if resource == 'locations':
      file_locations = file_resource
    elif resource == 'areas':
      file_areas= file_resource
    elif resource == 'regions':
      file_regions= file_resource
    else:
      print 'bad configuration file'
      exit (1)
  f_resources.close()

  f_location = codecs.open(dir_out+prefix+'_loc.txt', 'w',encoding='utf-8') 
  f_location_day = codecs.open(dir_out+prefix+'_location_day.csv', 'w',encoding='utf-8')
  f_location_day.write ('created_at;latitude;longitude\n')
  f_geolocation_day = codecs.open(dir_out+prefix+'_geolocation_day.csv', 'w',encoding='utf-8')
  f_geolocation_day.write ('Timestamp;latitude;longitude;text\n')
  location=Location(prefix,file_regions,file_areas,file_locations)
  num_tweets=0
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
       date= datetime.datetime.strftime(local_time,'%Y-%m-%d %H:%M')
       text=text.lower()
       author=author.lower()
       loc=loc.lower()
       num_tweets=num_tweets +1 
       if num_tweets % 10000 == 0:
          print num_tweets 
       if first_tweet == True:
         start_time= local_time
         first_tweet=False
       (name_spain,name_region,name_area,name_location,latitude,longitude) =location.get_location(loc)
       if (name_spain=='NA') and (name_region=='NA') and (name_area=='NA') and (name_location=='NA'):
         pass
       else:
         f_location.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %(line.strip("\n"),name_spain,name_region,name_area,name_location,latitude,longitude)) 
         if (latitude != 0) and (longitude !=0):
           f_location_day.write('%s;%s;%s\n' %  (date,latitude,longitude))
       geoloc=re.search(r'\t([0-9\.-]+),(\s[0-9\.-]+)',line,re.U)
       if geoloc:
         if  geoloc.group(1) != '0.0' and geoloc.group(2) != '0.0':
           f_geolocation_day.write ('%s;%s;%s;%s\n' %  (date,geoloc.group(2),geoloc.group(1),text))
           nun_tweets_geoloc +=1
  (per_location,statistics)=location.get_statistics () 
  end_time= datetime.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minutes), second=int(seconds))
  f_out = codecs.open(dir_out+prefix+'_loc_status.txt', 'w',encoding='utf-8')
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
  f_out.write('percentage tweets located\t%.2f%s\n' % (per_location,'%'))
  per_geoloc=(nun_tweets_geoloc*100.0)/num_tweets
  f_out.write('percentage tweets geolocated\t%.2f%s\n' % (per_geoloc,'%'))
  f_in.close()
  f_out.close()
  f_location.close() 
  f_geolocation_day.close()
  f_location_day.close()
  return
  exit(0)

if __name__ == '__main__':
  main() 