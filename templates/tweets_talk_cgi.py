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
import cgi
import cgitb
import codecs

  
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# put_html_urls
# author M.L. Congosto
# 1-mar-2012
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #    
  
def put_html_urls (source):

  source_with_html_urls=u''
  
#  print '\n\nbefore remove urls\n',source.encode( "utf-8" )
  #renove urls from tweet
  urls=re.findall (r'(http[s]*://\S+)', source,re.U)
  for url in urls:
#    print '\nUrl:',url
#    print '\nsource',source.encode( "utf-8" )
    start=source.find(url)
    end=len(url)
#    print '\n indices',start,end
    source_with_html_urls=source_with_html_urls+source[0:start-1]+' <a href="'+url+'" target="_blank">'+url+'</a> '
#    print '\nsource_without_url',source_without_urls.encode( "utf-8" )
    source=source[start+end:] 
  source_with_html_urls=source_with_html_urls+source
  #print source_with_html_urls
  return source_with_html_urls 

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# get_ranges
# author get code from http://sujitpal.blogspot.com/2007/04/building-tag-cloud-with-python.html
# 28-1-2011
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #   
 
def get_ranges(max_count, min_count):
  ranges = []
  distrib = (max_count - min_count+3)/ 4;
  index = min_count
  if distrib >0:
    while (index <= max_count):
      range_step = (index, index + distrib)
      index = index + distrib
      ranges.append(range_step)
  else:
    range=(1,1)
    ranges.append(range)   
  return ranges 

def main():
  print "Content-Type: text/html\n\n"
  list_tweets=[]
  list_select=[]
  max_sentences=40
  num_pages=10
  flag_select=False
  experimento=''
  source=''
  select='None'
  title=''
  #dir_base='/home/congosto/experimentos/tweet_store/'
  dir_base='/home/apt/mcongosto/lib/www'
  home='http://t-hoarder.com'
  script='tweets_talk_cloud_cgi.py'
  #cgitb.enable(display=0, logdir="/tmp") 
  cgitb.enable() 
  reload(sys)
  sys.setdefaultencoding('utf-8')
  sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
  form = cgi.FieldStorage()
  experiment=form.getvalue("experiment")
  source = form.getvalue("source")
  select = form.getvalue("select")
 
  file_source='%s/%s/%s_%s_sentences.csv' % (dir_base,experiment,experiment,source)
  if (select ==  None) or select=="None":
    pass
  else:
    flag_select= True
    file_select='%s/%s/%s_%s.txt' % (dir_base,experiment,experiment,select)
  most_spread='%s/scripts/%s?experiment=%s&source=global&select=%s' % (home,script,experiment,select)
  most_recent='%s/scripts/%s?experiment=%s&source=hoy&select=%s' % (home,script,experiment,select)
  print '''
  <html>
  <head>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
  <link href="http://t-hoarder.com/js/bootstrap/css/bootstrap.min.css" rel="stylesheet"/>
  <link rel="stylesheet" href="http://t-hoarder.com/css/dark_talk.css" type="text/css" media="screen" />
  </head>
  <body>
  <form>
  '''
  print '<input type="button" value="más recientes" name="tweets_hoy"  onclick="self.location.href=\'%s\'";>' % (most_recent)
  print '<input type="button" value="más difundidos" name="tweets_global" onclick="self.location.href=\'%s\'";>' % (most_spread)
  print '</form>'
  if source =='global':
    print '<div class="title"> tweets más difundidos<br/></div>'
  elif source =='hoy':
    print '<div class="title">tweets más recientes<br/></div>'
  else:
    print '<div class="title">tweets del %s<br/></div>' % (source)
  if flag_select:
    #print file_select
    f_select = codecs.open(file_select, 'rU',encoding='utf-8', errors='ignore')
    for line in f_select:
      word=line.strip("\n")
      list_select.append(word)
      #print word
  
  f_source = codecs.open(file_source, 'rU',encoding='utf-8', errors='ignore')
  max_count=0
  for line in f_source:
    data=line.split('\t')
    if len(data) == 5:
      date=data[0]
      author=data[1]
      text=data[2]
      count=int(data[3])
      id_tweet=data[4].strip("\n")
      if count > max_count:
        max_count= count
      min_count=count
      if not flag_select:
        list_tweets.append((text,author, count,date,id_tweet))
      else:
        flag_found=False 
        for word in list_select:
          text_lower=text.lower()
          author_lower=author.lower()
          #print flag_found,word,text,'</br>'
          if (text_lower.find (word) != -1 ) or (author_lower.find (word) != -1):
            #print 'found',word,text,id_tweet,'<br>',
            list_tweets.append((text,author, count,date,id_tweet))
            flag_found=True
            break
        
  num_pages=(len(list_tweets)+3)/4
  if num_pages >10:
    num_pages=10
  print '''
  <ul class="nav nav-pills">
  <li class="text-info">top&nbsp;</li>
  <li class="active"><a href="#pag-1" data-toggle="tab">1</a></li>
  '''
  for i in range (2,num_pages+1):
    print '<li class=""><a href="#pag-%s" data-toggle="tab">%s</a></li>' % (i,i)
  print '''
  <li class="text-info"> botton</li>
  </ul>
  <div class="tab-content">
  '''


  ranges=get_ranges(max_count,min_count)

  rangeStyle = ["smallestTag", "smallTag", "mediumTag", "largeTag", "largestTag"]
  background_list= ['<div id=\"box-gris\">','<div id=\"box-white\">']
 
  i_background= 0
  num_sentences=0
  num_sentences=0
  page=0
  print '<div class="tab-pane active" id="pag-1">'
  for  (text,author, count,date,id_tweet) in list_tweets:
    author_sin=author[1:]
    sentence= put_html_urls (text)
    background=  background_list[i_background]
    i_background = (i_background + 1) % 2
    #print count,text
    if num_sentences > max_sentences:
      break
    rangeIndex = 0
    if len(ranges) >0:
      for range_step in ranges:
        if (count >= range_step[0] and count <= range_step[1]):
          if (num_sentences) % 4 == 0:
            #print num_sentences
            page += 1
            #print 'pagina',page
            if page > 1:
              print '\n</div>'
              print '<div class="tab-pane fade" id="pag-%s">' %(page)
          #print type (id_tweet), id_tweet
          if id_tweet =='None':
            print '%s <span class="%s" > <span class="negrita"> <a href=https://twitter.com/#!/%s target="_blank"> %s </a> </span><br/>%s<br/><span class="negrita">  %s </span>, Propagado: <span class="negrita"> %s </span> veces</span></div>' % (background,rangeStyle[rangeIndex],author_sin,author,sentence,date,str(count))
          else: 
            print '%s <span class="%s" > <span class="negrita"> <a href=https://twitter.com/#!/%s target="_blank"> %s </a> </span><br/>%s<br/><span class="negrita"> <a href=https://twitter.com/%s/status/%s target="_blank"> %s </a> </span>, Propagado: <span class="negrita"> %s </span> veces</span></div>' % (background,rangeStyle[rangeIndex],author_sin,author,sentence,author_sin,id_tweet,date,str(count))
          num_sentences = num_sentences + 1  
          break 
        rangeIndex = rangeIndex + 1


  print '''
  </div>
  </div>
  <!--javascript>
  ================================================== -->
  <!-- Placed at the end of the document so the pages load faster -->
  <script src="http://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js"></script>
  <script src="http://t-hoarder.com/js/bootstrap/js/bootstrap.js"></script>
  </body>
  </html>
  '''

  f_source.close()
  if flag_filter:
    f_filter.close()
if __name__ == '__main__':
  main()