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
  def __init__(self,  experiment,dir_in,dir_out):
    self.experiment=experiment
    self.dir_in= dir_in
    self.dir_out= dir_out
    self.dict_status={}
    return

  def status(self,pack,file_status):
    file_in='%s/streaming_%s_%s_%s' % (self.dir_in,self.experiment,pack, file_status)
    #print '---> open file',file_in
    try:
      f_in=codecs.open(file_in, 'rU',encoding='utf-8')
    except:
      print 'can not open',file_in
      return

    list_status=[]
    for line in f_in:
      line=line.strip('\n')
      data=line.split('\t')
      list_status.append(data[1])
    f_in.close()
    self.dict_status[int(pack)]=list_status
    print file_status,list_status
    return
    
      
  def get_status (self,file_status):

    file_out='%s/%s_%s' % (self.dir_out,self.experiment,file_status)
    f_out=codecs.open(file_out,'w',encoding='utf-8')
    f_out.write ('pack\trun time\tstart\tstop\tlast tweet\tfile_lenght\tnum tweets\n')
    for i in range (0, len(self.dict_status)):
      data=self.dict_status[i]
      f_out.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % ( i,data[6],data[0],data[1],data[3],data[4],data[5]))
    return
    
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# main
# author M.L. Congosto
# 23-jul-2015   
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

def main():
    
  #get parameters
  files_status=['counter_status.txt','talk_status.txt','loc_status.txt']

  parser = argparse.ArgumentParser(description='This script join status of block procesed')
  parser.add_argument('experiment', type=str, help='experiment name')
  parser.add_argument('--dir_in', type=str, default='./', help='Dir data input')
  parser.add_argument('--dir_out', type=str, default='./', help='Dir data output')
  args = parser.parse_args()

  dir_in=args.dir_in
  dir_out=args.dir_out
  experiment=args.experiment
  file_log='%s/%s_status.txt' % (dir_out,experiment)
  f_log=codecs.open(file_log,'w',encoding='utf-8')
  num_pack=0
  # joining counters
  joining=JoinCounters(experiment,dir_in,dir_out)
  start = datetime.datetime.fromtimestamp(time.time())
  print 'processing counters file_counters_top'
  for file_status in  files_status:
    while True:
      pack='%s/streaming_%s_%s_%s' % (dir_in,experiment,num_pack,file_status)
      (status, output) =commands.getstatusoutput('ls '+pack)
      #print status, output
      if status !=0:
        break
      joining.status(num_pack,file_status)
      print 'processed', pack
      num_pack += 1
    joining.get_status(file_status)
    num_pack=0
  stop = datetime.datetime.fromtimestamp(time.time())
  f_log.write(('total runtime\t%s\n') % (stop - start))
  f_log.close()
  exit(0)

if __name__ == '__main__':
  main()