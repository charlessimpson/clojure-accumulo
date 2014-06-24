(defproject clojure-accumulo/clojure-accumulo "0.2.0-SNAPSHOT"
  :description "Clojure bindings for Apache Accumulo"
  :url "https://github.com/charlessimpson/clojure-accumulo"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.apache.hadoop/hadoop-client "1.2.1"
                   :exclusions [ant
                                hsqldb
                                junit
                                org.mortbay.jetty
                                org.codehaus.jackson/jackson-core-asl
                                tomcat]]
                 [org.apache.zookeeper/zookeeper "3.3.6"
                   :exclusions [com.sun.jmx/jmxri
                                com.sun.jdmk/jmxtools
                                log4j
                                javax.jms/jms
                                jline/jline]]
                 [org.apache.accumulo/accumulo-core "1.5.1"
                   :exclusions [jline
                                junit
                                org.apache.httpcomponents/httpcore]]
                 [org.slf4j/slf4j-log4j12 "1.7.6"]
                 ])
