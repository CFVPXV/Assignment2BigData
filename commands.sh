#!/bin/zsh
hadoop com.sun.tools.javac.Main PairCountPart2.java

jar cf wc.jar PairCountPart2*.class

hadoop jar wc.jar PairCountPart2 input output
