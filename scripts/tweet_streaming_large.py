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
import tweepy
from getpass import getpass
from textwrap import TextWrapper
import codecs
import argparse

class oauth_keys(object):
  def __init__(self,  app_keys_file,user_keys_file):
    self.matrix={}
    self.app_keys_file = app_keys_file
    self.user_keys_file = user_keys_file
    app_keys=[]
    user_keys=[]
    f = open(self.app_keys_file, 'rU')
    for line in f: 
      app_keys.append(line[:-1])
#    print app_keys
    f.close()
    f = open( self.user_keys_file, 'rU')
    for line in f: 
      user_keys.append(line[:-1])
    f.close()
    try: 
      self.auth = tweepy.OAuthHandler(app_keys[0],app_keys[1])
      self.auth.secure = True
      self.auth.set_access_token(user_keys[0], user_keys[1])
    except:
      print 'Error in oauth autentication, user key ', user_keys_file_num
      exit(83)
    return
  def get_auth(self):
    return self.auth
    
class Files_output(object):
  def __init__(self,dir_dest,prefix,ext):
    print 'Init Files_output' , dir_dest,prefix,ext
    self.dir_dest= dir_dest
    self.prefix=prefix
    self.ext=ext
    self.file_number = self.dir_dest + self.prefix+'.number'
    self.pack_number=0
    self.status='remaining'       
    try:
      self.f_number= open(self.file_number, 'rU')
    except:
      print 'First time, file number = 0'
      self.status='starting'
      self.f_number= open(self.file_number, 'w')
      self.f_number.write('0\n') 
      self.f_number.close()
      self.f_number= open(self.file_number, 'rU')
      
    for line in self.f_number:
      self.pack_number=int(line[:-1])
    self.f_number.close()

    self.file_out = self.dir_dest + self.prefix+'_'+ str(self.pack_number) +'.'+self.ext
    self.file_log = self.dir_dest + self.prefix+'.log'  
      
    try:
      self.f_out= codecs.open(self.file_out, 'a',encoding='utf-8')
    except:
      print 'error opening', self.file_out
      exit (1)
      
    try:
      self.f_log= codecs.open(self.file_log, 'a',encoding='utf-8')
    except:
      print 'error opening', self.file_log 
      exit (1)
    self.f_log.write (('====================> file %s  %s at %s\n') % ( self.file_out,self.status, datetime.datetime.now()))
    self.f_log.flush()
    self.status='remaining'  

 
  def write_out (self, item):
    try:
      self.f_out.write (item)
    except:
      print 'error writing', self.file_out, 'num_tweet',self.n_tweets
  
  def size_f_out (self):
    return self.f_out.tell() 

  def write_log (self, item):
    try:
      self.f_log.write (item)
      self.f_log.flush()
    except:
      print 'error writing', self.file_log  
      
  def write_number (self, item):
    try:
      self.f_out.write (item)
    except:
      print 'error writing', self.file_num 
      exit (1)
      
  def new_pack (self):
    #escribo primero el numero de fichero por si se muere el en tar no machaque el fichero actual al renacer
    self.f_number= open( self.file_number,'w')  
    self.f_number.write(('%s\n') % ( self.pack_number+1)) 
    self.f_number.flush()
    self.f_number.close() 
 #  file closing and compressing and new file 
    self.f_out.close()
    command='tar -czvf %s.tar.gz %s\n' % (self.file_out,self.file_out)
    print command
    os.system(command)
    command= 'rm %s\n' % (self.file_out)
    print command
    os.system(command)
 #open new file       
    self.pack_number= self.pack_number+1
    self.file_out = self.dir_dest + self.prefix+'_'+str (self.pack_number) +'.'+self.ext
    self.f_out= codecs.open(self.file_out, 'w',encoding='utf-8') 

#based on joshthecoder example http://github.com/joshthecoder/tweepy-examples/blob/master/streamwatcher.py
class StreamWatcherListener(tweepy.StreamListener):

  def __init__(self,dir_dest,prefix,ext,auth):
    self.files=Files_output(dir_dest,prefix,ext)
    self.start_time= time.time()
    self.last_time= self.start_time
    self.n_tweets=0 
    self.MAX_SIZE=100000000  
    self.api = tweepy.API(auth)
    
  def on_status(self, status):
    url_expanded =None
    geoloc=None
    location=None
    url_media=None
    type_media=None
    url_expanded=None
    description=None
    name=None

    if status.coordinates:
      #print status.coordinates
      coordinates=status.coordinates
      list_geoloc = coordinates['coordinates']
      geoloc= '%s, %s' % (list_geoloc[0],list_geoloc[1])
    if status.entities:
      #print status.entities
      entities=status.entities
      urls=entities['urls']
      if len (urls) >0:
        url=urls[0]
        url_expanded= url['expanded_url']
        #print '\nencontrada url', url_expanded,status.text
      if 'media' in entities:
        list_media=entities['media']
        if len (list_media) >0:
          media=list_media[0]
          url_media= media['media_url']
          #print '\nencontrada url media', url_media,status.text
          type_media=media['type']
    text=re.sub('[\r\n\t]+', ' ',status.text,re.UNICODE)
    if status.user.location:
      location=re.sub('[\r\n\t]+', ' ',status.user.location,re.UNICODE)
    if status.user.description:
      description=re.sub('[\r\n\t]+', ' ',status.user.description,re.UNICODE)
    if status.user.name:
      name=re.sub('[\r\n\t]+', ' ',status.user.name,re.UNICODE)
    try:
      tweet= '%s\t%s\t@%s\t%s\tvia=%s\tid=%s\tfollowers=%s\tfollowing=%s\tstatuses=%s\tloc=%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' %  (status.id,status.created_at,status.author.screen_name,text, status.source,status.user.id,  status.user.followers_count,status.user.friends_count,status.user.statuses_count,location,url_expanded, geoloc,name,description, url_media,type_media,status.lang)
      #print tweet
      self.files.write_out(tweet)
    except:
#      print 'paso por posible unicode error\n'
      text_error = '---------------> posible unicode error  at %s, id-tweet %s\n' % ( datetime.datetime.now(),status.id)
      self.files.write_log (text_error)
# Catch any unicode errors while printing to console
# and just ignore them to avoid breaking application.
      print text_error
      pass
      
           
    self.n_tweets= self.n_tweets + 1 
    if self.n_tweets % 10000 == 0:
      current_time=time.time()
      average_rate= self.n_tweets /(current_time - self.start_time)
      last_rate= 10000 /(current_time - self.last_time)
      msj_log='store %s at %s, last rate %s average rate %s \n' % (self.n_tweets, datetime.datetime.now(), last_rate, average_rate)
      self.files.write_log (msj_log)
      self.last_time=current_time
      
      if  self.files.size_f_out() >= self.MAX_SIZE:
        print 'new pack'
        self.files.new_pack ()  #  increase file number 

  def on_error(self, status_code):
 #   print 'paso por on_error\n'
    text_error = '---------------->An error has occured! Status code = %s at %s\n' % (status_code,datetime.datetime.now())
    self.files.write_log (text_error)
    print text_error
    return True # keep stream alive
 
  def on_timeout(self):
 #   print 'paso por on_timeout\n'
    text_error = 'Snoozing Zzzzzz at %s\n' % ( datetime.datetime.now())
    self.files.write_log (text_error)
    print text_error
    return False #restart streaming

def get_list (file_list):
  try:
    f_list= open(file_list,'rU')
  except:
    print "file doesn't exist:", file_list
    exit(1)
  list_plana= f_list.read()
  list_datos= list_plana[:-1].split (',')
  return list_datos
    

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# main
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def main():
  follow_list=None
  track_list=None
  locations_list=None
  locations_list_int=[]
#entorno
  
  reload(sys)
  sys.setdefaultencoding('utf-8')
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
  parser = argparse.ArgumentParser(description='get tweets from Streaming API ')
  parser.add_argument('root', type=str, help='  t-hoarder root')
  parser.add_argument('experiment', type=str, help='name experiment')
  parser.add_argument('app_keys', type=str, help='file with keys app')
  parser.add_argument('user_keys', type=str,help='file with user token access')
  group = parser.add_mutually_exclusive_group()
  group.add_argument('--users', type=str, default='', help='get tweets for id_tweets')
  group.add_argument('--words', type=str, default='', help='get tweets for keywords')
  group.add_argument('--locations', type=str, default='', help='get tweets for location')
  args = parser.parse_args()
  
  path_root=args.root
  experiment=args.experiment 
# enviroment
  path_keys=path_root+'/t-hoarder/keys/'
  path_store=path_root+'/t-hoarder/store/'
  path_experiment=path_root+'/t-hoarder/store/'+experiment+'/'
# end enviroment
  app_keys_file= path_keys+args.app_keys
  user_keys_file= path_keys+args.user_keys
  file_users=args.users
  file_words=args.words
  file_location = args.locations
  if file_users != '':
    follow_list=get_list (path_experiment+file_users)
  if file_words != '':
    track_list=get_list (path_experiment+file_words)
  if file_location != '':
     locations_list=get_list (path_experiment+file_location)
     for location in locations_list:
        locations_list_int.append (float(location))
  file_out=path_store+'streaming_'+experiment+'.txt'
  filename=re.search (r"([\w-]+)\.([\w]+)*", file_out)
  if not filename:
    print "%s bad filename, it must have an extension xxxx.xxx",file_out
    exit (1)
  prefix= filename.group(1)
  ext= filename.group(2)
  print '-->File output: ', file_out
  oauth=oauth_keys(app_keys_file,user_keys_file)
  auth=oauth.get_auth()
  stream = tweepy.Stream(auth, StreamWatcherListener(path_store,prefix,ext,auth), timeout=None)  
    # Prompt for mode of streaming
  print follow_list,track_list,locations_list_int
  stream.filter(follow_list, track_list,False,locations_list_int)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print '\nGoodbye!'
