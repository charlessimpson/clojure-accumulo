(ns clojure-accumulo.accumulo
  "Basic operations in Accumulo"
  (:import [org.apache.accumulo.core.client ScannerBase ZooKeeperInstance]
           [org.apache.accumulo.core.client.mock MockInstance]
           [org.apache.accumulo.core.security Authorizations ColumnVisibility]
           [org.apache.hadoop.io Text]))

(def
  #^{:dynamic true
     :doc "Connector against which the current scan/write/delete should be run"}
  *conn*)

(def
  #^{:dynamic true
     :doc "Authorizations the current scan/delete should be executed with"}
  *auths* (Authorizations.))

(def
  #^{:dynamic true
     :doc "Maximum latency (milliseconds) when writing or deleting.  Defaults 
          to maximum allowable time for server."}
  *max-latency* 0)

(def
  #^{:dynamic true
     :doc "Maximum memory (bytes) to batch before writing or deleting."}
  *max-memory* 1048576)

(def
  #^{:dynamic true
     :doc "Number of threads to spawn when reading or deleting.  Defaults to 1.
          "}
  *read-threads* 1)

(def
  #^{:dynamic true
     :doc "Maximum number of threads to spawn when writing or deleting.
          Defaults to 1."}
  *write-threads* 1)

(def
  #^{:dynamic true
     :private true
     :doc "Open batch scanners associated with a particular connector.  Don't
          touch this thing yourself.  Bound to nil initially so we can easily
          check whether someone bound it in with-connector."}
  *scanners* nil)

(defmulti
  #^{:doc "Cast value to Authorizations."}
  as-authorizations class)

(defmethod as-authorizations Authorizations [x]
  x)

(defmethod as-authorizations nil [x]
  (Authorizations.))

(defmethod as-authorizations String [x]
  (Authorizations. (.getBytes ^String x)))

(defmethod as-authorizations clojure.lang.IPersistentCollection [x]
  (Authorizations. ^java.util.Collection
                   (map (fn [^String s] (.getBytes s)) x)))

(defmulti
  #^{:doc "Cast value to ColumnVisibility."}
  as-visibility class)

(defmethod as-visibility ColumnVisibility [x]
  x)

(defmethod as-visibility String [x]
  (ColumnVisibility. ^String x))

(defn create-instance
  "Create an Accumulo instance from an options map.  The map should contain:
    :instance-name    Accumulo instance name
    :zoo-keepers      comma separated list of ZooKeepers
  or:
    :mock?            if true, create a MockInstance
  "
  [{:keys [mock? instance-name zoo-keepers]}]
  (if mock?
    (MockInstance.)
    (ZooKeeperInstance. instance-name zoo-keepers)))

(defn create-connector
  "Create an Accumulo connector from an options map.  It must contain:
    :user     user name
    :password user password

  It must contain on of the following:
    :instance an Accumulo instance
  or the set of keys specified in create-instance."
  [{:keys [user password instance instance-name zoo-keepers mock?] :as opts}]
  (.getConnector (if instance
                   instance
                   (create-instance opts))
                 user password))

(defn create-scanner
  "Create a Scanner in the current connector context."
  [^String table]
  (.createScanner *conn* table *auths*))

(defn create-batch-scanner
  "Create a BatchScanner in the current connector context."
  [^String table]
  (let [s (.createBatchScanner *conn* table *auths* *read-threads*)]
    (if @*scanners*
      (swap! *scanners* conj s))
    s))

(defn create-batch-writer
  "Create a BatchWriter in the current connector context."
  [^String table]
  (.createBatchWriter *conn* table *max-memory* *max-latency* *write-threads*))

(defn create-batch-deleter
  "Create a BatchDeleter in the current connector context."
  [^String table]
  (.createBatchDeleter *conn* table *auths* *read-threads* *max-memory*
                       *max-latency* *write-threads*))

(defn add-iterator
  "Apply iter map describin scan iterator to scanner s.  The iterator map must
  contain the following:
    :class    string giving fully qualified class name of iterator
    :name     iterator name
    :priority priority
  Any other key/value pairs in the iterator map are passed as options to the
  scan iterator.
  "
  [^ScannerBase s iter]
  (let [{:keys [priority class name]} iter]
    (.setScanIterators s priority class name)
    (for [[k v] (dissoc iter :priority :class :name)]
      (.setIteratorOption name (str k) (str v)))))

(defn add-iterators
  "Apply a sequence of iterators to a scanner.  Each entry in the sequence must
  be a map describing the iterator.  If the name is not given, it is derived 
  from the class of the iterator.  If the priority is not given, it is derived
  from the iterator's index in the sequence.
  "
  [^ScannerBase s iters]
  (for [iter iters ind (range)]
    (let [{:keys [class name priority]
           :or {name (gensym class) priority ind}} iter]
      (add-iterator s))))

(defn- limit-column-families
  "Limit the column families that a scanner returns.  column-familiies is a 
  sequence of Strings."
  [^ScannerBase s column-families]
  (doseq [cf column-families]
    (.fetchColumnFamily s (Text. cf))))

(defn scan
  "Scan a table over one or more ranges.  Ranges should be a collection of
  range, even if only one is desired.

  Optional arguments:
    :batch?           if true, a BatchScanner is used
    :column-families  a sequence of column families (Strings) to limit the scan
                      by
    :iterators        a sequence of scan iterators.  See add-iterators.
  "
  [table ranges & {:keys [batch? column-families iterators]}]
  (let [scanner (doto ((if batch? create-batch-scanner create-scanner) table)
                  (add-iterators iterators)
                  (limit-column-families column-families))]
    (if batch?
      (seq (doto scanner (.setRanges ranges)))
      (mapcat (fn [r]
                (seq (doto scanner (.setRange r))))
              ranges))))

(defn write
  "Write a collection of mutations to a table."
  [table coll]
  (with-open [wtr (create-batch-writer table)]
    (doseq [mutation coll]
      (.addMutation wtr mutation))))

(defn delete
  "Delete from a table.

  Optional arguments:
    :iterators  a sequence of scan iterators.  See add-iterators.
  "
  [table ranges & {:keys [iterators]}]
  (.delete (doto (create-batch-deleter table)
             (add-iterators iterators)
             (.setRanges ranges))))

(defn with-connector*
  [spec func]
  (binding [*conn* (create-connector spec)
            *scanners* (atom [])]
    (let [r (func)]
      (if *scanners*
        (doseq [s @*scanners*] (.close s)))
        r)))

(defmacro with-connector
  "Evalutates body in the context of a new connector to an Accumulo instance.
  spec is an options map passed to create-connector."
  [spec & body]
  `(with-connector* ~spec (fn [] ~@body)))

(defn with-authorizations*
  [auths func]
  (binding [*auths* (as-authorizations auths)]
    (func)))

(defmacro with-authorizations
  [auths & body]
  `(with-authorizations* ~auths (fn [] ~@body)))

(defn- table-ops
  []
  (.tableOperations *conn*))

(defn delete-table!
  "Delete table."
  [table]
  (.delete (table-ops) table))

(defn table?
  "Returns true if table exists, false otherwise."
  [table]
  (.exists (table-ops) table))

(defn create-table
  "Create table."
  [table]
  (.create (table-ops) table))

(defn create-user
  "Create user."
  ([user password]
   (create-user user password *auths*))
  ([user password authorizations]
   (.createUser (.securityOperations *conn*)
                user
                (.getBytes password)
                (as-authorizations authorizations))))

(defn delete-user!
  "Delete user."
  [user]
  (.dropUser (.securityOperations *conn*)))

(defn set-properties!
  "Set a collection of properties on a table.  propmap should be a map or a
  sequence of operty/value pairs."
  [table propmap]
  (doseq [[property value] propmap]
    (.setProperty (table-ops) table property value)))

(defn remove-properties!
  "Remove a collection of properties from a table."
  [table properties]
  (doseq [property properties]
    (.removeProperty (table-ops) table property)))

(defn set-property
  "Set a single property on a table."
  [table property value]
  (set-properties! table {property value}))

(defn remove-property
  "Remove a single property from a table."
  [table property]
  (remove-properties! table [property]))

(defn get-properties
  "Get properties of a table."
  [table]
  (into
    (array-map)
    (.getProperties (table-ops) table)))

(defn add-splits!
  "Add a collection of splits to a table."
  [table coll]
  (.addSplits
    (table-ops)
    table
    (java.util.TreeSet. (map #(Text. %) coll))))

(defn get-splits
  "Get table splits."
  ([table]
   (.getSplits (table-ops) table))
  ([table maxsplits]
   (.getSplits (table-ops) table maxsplits)))
