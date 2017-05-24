#! /bin/bash

dir=${0%/*};

mongo --port 27018 "${dir}/initReplicaset.js"