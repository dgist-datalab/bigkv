#!/bin/bash

ps -ef | grep blktrace | grep -v grep | awk '{print "kill -2 " $2}' | sh
