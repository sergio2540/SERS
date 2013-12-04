#!/bin/bash
superName="superadmin"
adminName="admin"
userName="user1"

baseDir=`~`
superDir=`readlink -f ~/Desktop/superadmin`
adminDir=`readlink -f ~/Desktop/admin`
userDir=`readlink -f ~/Desktop/user1`
relocation="../../"


mkdir $superDir
mkdir $adminDir
mkdir $userDir
pwd
x-terminal-emulator -e java -cp bin:lib/\* chord.Client $superName $superDir &
sleep 5
x-terminal-emulator -e java -cp bin:lib/\* chord.Client $adminName $adminDir &
sleep 5
x-terminal-emulator -e java -cp bin:lib/\* chord.Client $userName $userDir &
pwd

