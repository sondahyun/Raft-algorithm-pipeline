#!/bin/bash

bash simulation.sh 
bash runClient.sh 

sleep 15

bash closeClient.sh
bash stop.sh