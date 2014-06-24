(defproject clojure-accumulo/clojure-accumulo "0.2.0-SNAPSHOT"
  :description "Clojure bindings for Apache Accumulo"
  :url "https://github.com/charlessimpson/clojure-accumulo"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.apache.hadoop/hadoop-core "0.20.2"
                  :exclusions [ant/ant
                               junit/junit
                               org.mortbay.jetty/jetty
                               org.mortbay.jetty/jetty-util
                               org.mortbay.jetty/jsp-2.1
                               org.mortbay.jetty/jsp-api-2.1
                               org.mortbay.jetty/servlet-api-2.5
                               tomcat/jasper-compiler
                               tomcat/jasper-runtime]]
                 [org.apache.zookeeper/zookeeper "3.3.3"
                   :exclusions [com.sun.jmx/jmxri
                                com.sun.jdmk/jmxtools
                                javax.jms/jms
                                jline/jline]]
                 [org.apache.accumulo/accumulo-core "1.3.6"
                  :exclusions [jline/jline
                               junit/junit]]
                 ])
