(ns clojure-accumulo.accumulo-test
  (:use clojure.test
        clojure-accumulo.accumulo)
  (:import [org.apache.accumulo.core.data Key Range]))

(def db {:user "user" :password "password" :mock? true})

(deftest test-connector-binding
  (with-connector db
    (is (instance? org.apache.accumulo.core.client.Connector *conn*))))

(deftest test-table-operations
  (with-connector db

    (is (table? "!METADATA"))
    (is (not (table? "non-existent")))

    (create-table "new_table")
    (is (table? "new_table"))

    (delete-table! "new_table")
    (is (not (table? "new_table")))
    
    ))

(defn as-unversioned-key
  [^Key k]
  {:row (str (.getRow k))
   :cf  (str (.getColumnFamily k))
   :cq  (str (.getColumnQualifier k))
   :vis (str (.getColumnVisibility k))
   })

(deftest test-scan
  (with-connector db
    (create-table "demo")
    (write "demo"
           [(doto (org.apache.accumulo.core.data.Mutation. "r_a")
              (.put "cf_1" "cq_b" (as-visibility "") ""))
            (doto (org.apache.accumulo.core.data.Mutation. "r_b")
              (.put "cf_1" "cq_a" (as-visibility "") "")
              (.put "cf_1" "cq_c" (as-visibility "") "")
              (.put "cf_2" "cq_c" (as-visibility "") ""))
            (doto (org.apache.accumulo.core.data.Mutation. "r_c")
              (.put "cf_1" "cq_b" (as-visibility "") "")
              (.put "cf_2" "cq_b" (as-visibility "") ""))
            ])
    (letfn [(xkeys [entries] (map (comp as-unversioned-key key) entries))]
      (is (= (xkeys (scan "demo" [(Range. "r_0")]))
             []))

      (is (= (xkeys (scan "demo" [(Range. "r_a")]))
             [{:row "r_a" :cf "cf_1" :cq "cq_b" :vis ""}]))

      (is (= (xkeys (scan "demo" [(Range. "r_b")] :columns [["cf_1" "cq_a"]]))
             [{:row "r_b" :cf "cf_1" :cq "cq_a" :vis ""}]))

      (is (= (xkeys (scan "demo" [(Range. "r_c")] :column-families ["cf_1"]))
             [{:row "r_c" :cf "cf_1" :cq "cq_b" :vis ""}]))

    )))

(deftest test-iterators
  (with-connector db
    (create-table "demo")
    (write "demo"
      [(doto (org.apache.accumulo.core.data.Mutation. "r_a")
         (.put "cf_1" "cq_b" (as-visibility "") ""))
       (doto (org.apache.accumulo.core.data.Mutation. "r_b")
         (.put "cf_1" "cq_a" (as-visibility "") "")
         (.put "cf_1" "cq_c" (as-visibility "") "")
         (.put "cf_2" "cq_c" (as-visibility "") ""))
       (doto (org.apache.accumulo.core.data.Mutation. "r_c")
         (.put "cf_1" "cq_b" (as-visibility "") "")
         (.put "cf_2" "cq_b" (as-visibility "") ""))
       ])
    (letfn [(xkeys [entries] (map (comp as-unversioned-key key) entries))]
      (is (= (xkeys (scan "demo" [(org.apache.accumulo.core.data.Range.)]
                      :iterators [{:class org.apache.accumulo.core.iterators.user.GrepIterator
                                   :priority 10
                                   :name "grep"
                                   :term "cq_c"}]))
            [{:row "r_b" :cf "cf_1" :cq "cq_c" :vis ""} {:row "r_b" :cf "cf_2" :cq "cq_c" :vis ""}])))))
