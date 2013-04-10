# clojure-accumulo

A Clojure binding for [Apache Accumulo](http://accumulo.apache.org), "a sorted,
distributed key/value store... built on Apache
[Hadoop](http://hadoop.apache.org), [ZooKeeper](http://zookeeper.apache.org),
and [Thrift](http://thrift.apache.org)."

## Basic Usage

```clj
(def db {:mock? true})
(def auths "A,B,C")

(with-connector db
  (with-authorizations auths
    (create-table "demo")
    (write "demo"
           [(doto (org.apache.accumulo.core.data.Mutation. "r1")
              (.put "" "" (as-visibility "A") "Hello!"))
            (doto (org.apache.accumulo.core.data.Mutation. "r2")
              (.put "" "" (as-visibility "D") "Hidden")
              (.put "" "" (as-visibility "B") "Goodbye!"))])
    (doseq [[k v] (scan "demo" [(org.apache.accumulo.core.data.Range.)])]
      (prn (str (.getRow k)) (str (.getColumnVisibility k)) (str v)))))
```

## Upcoming

An API simplification!  [Elimination of
DSSR's](http://stuartsierra.com/2013/03/29/perils-of-dynamic-scope)!  In all
seriousness, we expect that the 0.2 series will maintain many of the concepts
in 0.1, but will break the API.  In particular,

  * pass connectors or connector descriptions into functions directly
  * eliminate the `with-connector` and `with-authorizations` macros
  * provide functions to simplify the keys/value pairs passed into scanners
  * give callers control over the lifetimes of their `BatchScanner` and
    `BatchWriter`
  * provide an abstractions for `Range`, `Key`, and `Mutation` so they don't
    have to be explicitly constructed
