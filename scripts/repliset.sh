#! /bin/bash

dir=${0%/*};

if [ ! -d ${dir}/data ]
then
	mkdir -p ${dir}/data/rs0-{0..2}
fi

mongod --port 27018 --dbpath ${dir}/data/rs0-0 --replSet rs0 --smallfiles --oplogSize 128 > /dev/null &

mongod --port 27019 --dbpath ${dir}/data/rs0-1 --replSet rs0 --smallfiles --oplogSize 128 > /dev/null &

mongod --port 27020 --dbpath ${dir}/data/rs0-2 --replSet rs0 --smallfiles --oplogSize 128 > /dev/null &
