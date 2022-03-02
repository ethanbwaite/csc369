hadoop fs -mkdir /user/$(whoami)/input
hadoop fs -copyFromLocal *.csv /user/$(whoami)/input
