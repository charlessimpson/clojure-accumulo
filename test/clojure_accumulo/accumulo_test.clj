(ns clojure-accumulo.accumulo-test
  (:use clojure.test
        clojure-accumulo.accumulo))

(deftest connector-binding
  (with-connector
    {:user "user" :password "password" :mock? true}
    (is (instance? org.apache.accumulo.core.client.Connector *conn*))))

(deftest table-operations
  (with-connector
    {:user "user" :password "password" :mock? true}

    (is (table? "!METADATA"))
    (is (not (table? "non-existent")))

    (create-table "new_table")
    (is (table? "new_table"))

    (delete-table! "new_table")
    (is (not (table? "new_table")))
    
    ))
