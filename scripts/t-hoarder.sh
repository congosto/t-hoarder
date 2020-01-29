#!/bin/bash
#Copyright 2016 Almudena Garcia Jurado-Centurion

#This program is free software: you can redistribute it and/or modify
#it under the terms of the GNU General Public License as published by
#the Free Software Foundation, either version 3 of the License, or
#(at your option) any later version.

#This program is distributed in the hope that it will be useful,
#but WITHOUT ANY WARRANTY; without even the implied warranty of
#MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#GNU General Public License for more details.

#You should have received a copy of the GNU General Public License
#along with this program.  If not, see <http://www.gnu.org/licenses/>.

opcion=7
exit='n'


echo "----------------------------------------"
echo "------>Welcome to t-hoarder      <------"
echo "----------------------------------------"
enviroment=False
while test ${enviroment} != 'True'
do
  echo "Type the root directory where you installed t-hoarder: "
  read root
  if [ -d ${root}/t-hoarder ]
  then 
    cd ${root}/t-hoarder
    enviroment='True'
  else
    echo "${root}/t-hoarder does not exist"
    enviroment='False'
  fi
done
dir_scripts=${root}/t-hoarder/scripts/
while test ${exit} != 's'
do

    echo "------------------------------"
    echo "Options:"
    echo "------------------------------"
    echo "1.Create credentials to access the API"
    echo "2.Create a new experiment"
    echo "3.Start an experiment"
    echo "4.Stop an experiment"
    echo "5.Experiment status(not implemented yet)"
    echo "6.Modify an experiment (not implemented yet)"
    echo "7.Delete an experiment (not implemented yet)"
    echo "8.T-hoarder status (not implemented yet)"
    echo "9.Exit"
    echo " "
    echo "--> Type the option number: "
    echo " "

    read opcion

    case $opcion in
        1)
            echo "Before creating the credentials you must:"
            echo "1. Create a Twitter app (https://apps.twitter.com) see instructions on the wiki
  (https://github.com/congosto/t-hoarder/wiki/Create-credits-to-access-a- The-API\)"
            echo "2. Save Consumer Key & Consumer Secret (one key per line without leaving spaces or tabs)
  in a file with the name of the app and the extension 'key' in the directory keys:"
            echo "3. Create a Twitter profile for each experiment"
            echo "If you have completed steps 1, 2 and 3 you can now create credentials for a Twitter 
  profile with the app"
            echo "-----Attention! Make sure all sessions are closed on Twitter----"
            echo "Enter the file name with the application keys: "
            read app_key
            echo "Enter the twitter profile: "
            read usuario
            cd keys            
            tweet_auth.py $app_key $usuario
            cd ..
        ;;

        2)
            echo "Enter the name of the experiment: "
            read experiment
            if [ -d ${root}/t-hoarder/${experiment} ];
            then
               echo "Experiment ${experiment} exists"
            else
               mkdir ${root}/t-hoarder/store/${experiment}
               echo "Enter the auhor's name: "
               read author
               echo "Autor: ${author}" > ./store/${experiment}/${experiment}.cfg
               echo "Enter the file name with the application keys: "
               read app_key
               echo "App keys: ${app_key}" >> ./store/${experiment}/${experiment}.cfg
               echo "Enter the twitter profile (with token): "
               read user_key
               echo "users keys: ${user_key}.key" >> ./store/${experiment}/${experiment}.cfg
               echo "Enter type filter (users | words | locations): "
               read type_filter
               bash make_panel  ${experiment}  ${author} ./templates  ./web/ > /dev/null
               case ${type_filter} in
                 users)
                   echo "A comma-separated list of user IDs. Take a look to API documentation -Follow-
  (https://dev.twitter.com/streaming/overview/request-parameters#follow):"
                   read list_users
                   echo "${type_filter}: ${list_users}" >> ./store/${experiment}/${experiment}.cfg
                   echo "${list_users}" > ./store/${experiment}/users.txt
                   echo "nohup bash tweet_streaming_persistent 'python2.7 ${dir_scripts}tweet_streaming_large.py ${root} ${experiment} $app_key $user_key.key --${type_filter} users.txt' &" > ./store/${experiment}/command_start 
                   echo "bash make_experiment ${root} ${experiment} 1" > ./store/${experiment}/command_process
                   echo "Experiment ${experiment} created"
                 ;;
                 words)
                   echo "A comma-separated list of phrases. Take a look to API documentation -Track-
  (https://dev.twitter.com/streaming/overview/request-parameters#track):"
                   read list_words
                   echo "${type_filter}: ${list_words}" >> ./store/${experiment}/${experiment}.cfg
                   echo "${list_words}" > ./store/${experiment}/words.txt
                   echo "nohup bash tweet_streaming_persistent 'python2.7 ${dir_scripts}tweet_streaming_large.py ${root} ${experiment} $app_key $user_key.key --${type_filter} words.txt' &" >  ./store/${experiment}/command_start
                   echo "bash make_experiment ${root} ${experiment} 1" > ./store/${experiment}/command_process
                   echo "Experiment ${experiment} created"
                 ;;
                 locations)
                   echo "A comma-separated list of longitude,latitude pairs specifying a set of bounding boxes. Take a look to API documentation -Locations-
  (https://dev.twitter.com/streaming/overview/request-parameters#locations):"
                   read list_locations
                   echo "${type_filter}: ${list_locations}" >> ./store/${experiment}/${experiment}.cfg
                   echo "${list_locations}" > ./store/${experiment}/locations.txt
                   echo "nohup tweet_streaming_persistent 'python2.7 ${dir_scripts}tweet_streaming_large.py ${root} ${experiment} $app_key $user_key.key --${type_filter} locations.txt' &" > ./store/${experiment}/command_start
                   echo "bash make_experiment ${root} ${experiment} 1" > ./store/${experiment}/command_process
                   echo "Experiment ${experiment} created"
                 ;;
                 *)
                    echo " ${type_filter} option unknow"
                    echo "Experiment ${experiment} NOT created"
                 ;;
               esac
            fi
        ;;

        3)    
            echo "Enter the name of the experiment: "
            read experiment
            if [ -e ${root}/t-hoarder/store/${experiment}/command_start ]
            then
               num_pids=`ps -e -o pid,args |grep -c ${experiment}`
               if [ ${num_pids} -eq 1 ]
               then
                 cd store/${experiment}
                 command=cat ./command_start
                 ${command} 
                 echo "${experiment} started"
                 cd ../../
               else
                 echo "Experiment ${experiment} is running"
               fi
            else
               echo "${experiment} does not created"
            fi
        ;;

        4)
            echo "Enter the name of the experiment: "
            read experiment
            if [ -e ${root}/t-hoarder/store/${experiment}/command_start ]
            then
               num_pids=`ps -e -o pid,args |grep -c ${experiment}`
               if [ ${num_pids} -eq 1 ]
               then
                 echo "Experiment ${experiment} is not running"
               else
                 pids=`ps -e -o pid,args |grep ${experiment} | grep -o '^[ ]*[0-9]*'`
                 for pid in $pids
                 do
                   kill -9 $pid 2>/dev/null
                 done
                 echo "Experiment ${experiment} stopped"
                 
               fi
            else
               echo "${experiment} does not created"
            fi
        ;;

        5)
        


        ;;
        6)
        

        ;;

        7)
        


        ;;

        8)
            salir='s'
        ;;
    esac
done
