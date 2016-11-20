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
import tweepy
import webbrowser
import argparse

def get_access_key(keys_app_file, username):
  keys_app=[]
  f = open(keys_app_file, 'rU')
  for line in f:
    this_line = line.rstrip()
    keys_app.append(this_line)

  print keys_app
  auth = tweepy.OAuthHandler(keys_app[0], keys_app[1])
  auth.secure = True

  # Open authorization URL in browser
  webbrowser.open(auth.get_authorization_url())

  # Ask user for verifier pin
  pin = raw_input('Verification pin number from twitter.com: ').rstrip()

  # Get access token
  print pin
  token = auth.get_access_token(verifier=pin)
  print token

  # Give user the access token
  print 'Access token:'
  print ' Key: %s' % token[0]
  print ' Secret: %s' % token[1]
  file_out= username+'.key'
  f_out=  open(file_out, 'w')  
  f_out.write ("%s\n" % ( token[0]))
  f_out.write ("%s\n" % ( token[1]))
  return

def main():
  parser = argparse.ArgumentParser(description='It gets the  access and secret key of a user')
  parser.add_argument('keys_app', type=str, help='file with keys app')
  parser.add_argument('user', type=str, help='twitter user')
  args = parser.parse_args()
  
  keys_app_file=args.keys_app  
  username= args.user 

  get_access_key(keys_app_file, username)
  exit(0)

if __name__ == '__main__':
  main()
