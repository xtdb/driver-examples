(ns load-data
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [clojure.tools.logging :as log]))

;; Database connection details (adjust as needed)
(def db-spec {:dbtype "xtdb"
              :dbname "xtdb"
              :host "xtdb"
              :user "xtdb"})

(defn read-tsv-files [dir]
  "Reads all TSV files from the given directory. Returns a map of table-name to file-path."
  (->> (file-seq (io/file dir))
       (filter #(.isFile %))
       (filter #(str/ends-with? (.getName %) ".tsv"))
       (reduce (fn [acc file]
                 (let [table-name (-> (.getName file)
                                      (str/replace #"\.tsv$" "")
                                      (str/replace #"^public_" ""))] ;; Remove public_ prefix
                   (assoc acc table-name (.getAbsolutePath file))))
               {})))

(defn parse-value [value]
  "Attempts to parse a value into a common data type (number, boolean, date, or string)."
  (cond
    ;; Handle numbers
    (re-matches #"-?\d+(\.\d+)?" value)
    (if (re-find #"\." value)
      (Double/parseDouble value)
      (Long/parseLong value))

    ;; Handle booleans
    (re-matches #"(?i)(t|true|f|false)" value)
    (boolean (or (= "t" (str/lower-case value))
                 (= "true" (str/lower-case value))))

    ;; Handle ISO and common date formats
    (re-matches #"\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?" value)
    (try
      (let [format (if (re-find #" " value)
                     "yyyy-MM-dd HH:mm:ss"
                     "yyyy-MM-dd")]
        (.parse (java.text.SimpleDateFormat. format) value))
      (catch Exception _ value)) ;; Fallback to string if parsing fails

    ;; Handle null-like values
    (= "\\N" value) nil

    ;; Default: return the original string
    :else value))

(defn parse-record [record]
  "Parses the values of a record into common data types."
  (into {} (map (fn [[k v]] [k (parse-value v)]) record)))

(defn is-join-table? [table-name]
  "Detects if a table is a join table based on its name pattern."
  (let [parts (str/split table-name #"_")]
    (= 2 (count parts))))
; FIXME, better to check for absence of column matching (str table-name "_id")

(defn generate-record [table-name line column-names]
  "Converts a TSV line to a map representation for use with RECORDS."
  (let [values (str/split line #"\t")
        raw-record (zipmap column-names values)
        parsed-record (parse-record raw-record)]
    (if-not (= (count values) (count column-names))
      (throw (ex-info "Column count mismatch" {:table table-name
                                               :expected column-names
                                               :actual values})))
    (cond
      ;; Handle join tables
      (is-join-table? table-name)
      (let [[tbl1 tbl2] (str/split table-name #"_")
            tbl1-id (get parsed-record (str tbl1 "_id"))
            tbl2-id (get parsed-record (str tbl2 "_id"))]
        (assoc parsed-record "_id" (str tbl1-id "_" tbl2-id)))

      ;; Handle regular tables with `tbl_id` as PK
      :else
      (let [tbl-id-key (str table-name "_id")]
        (if-let [tbl-id (get parsed-record tbl-id-key)]
          (-> parsed-record
              (dissoc tbl-id-key)
              (assoc "_id" tbl-id))
          parsed-record)))))

(defn insert-tsv-into-db! [conn table-name file-path]
  "Reads a TSV file, processes its contents in batches, and inserts data into the database using RECORDS."
  (log/info "Processing file for table:" table-name)
  (with-open [reader (io/reader file-path)]
    (let [lines (line-seq reader)]
      (if (empty? lines)
        (log/warn "File is empty, skipping table:" table-name)
        (let [header (vec (str/split (first lines) #"\t"))
              records (map #(generate-record table-name % header) (rest lines))
              batch-size 1000 ;; Batch size for transactions
              total-count (->> records
                               (partition-all batch-size)
                               (transduce
                                 (map (fn [record-batch]
                                        (jdbc/with-transaction [tx conn]
                                          (with-open [ps (jdbc/prepare tx [(str "INSERT INTO " table-name " RECORDS ?")])]
                                            (jdbc/execute-batch! ps (map vector record-batch))))
                                        (count record-batch)))
                                 + 0))]
          (log/debug "Finished inserting" total-count "records for table:" table-name))))))


(defn process-tsv-files [conn dir]
  "Reads and inserts all TSV files from the directory into the database."
  (let [tsv-files (read-tsv-files dir)]
    (if (empty? tsv-files)
      (log/warn "No TSV files found in directory:" dir)
      (doseq [[table-name file-path] tsv-files]
        (log/info "Inserting data for table:" table-name "from file:" file-path)
        (try
          (insert-tsv-into-db! conn table-name file-path)
          (log/info "Finished inserting data for table:" table-name)
          (catch Exception e
            (log/error e "Error inserting data for table:" table-name)))))))

(defn -main [& args]
  (if (not (seq args))
    (log/error "Usage: clj -M -m main <path-to-tsv-directory>")
    (let [dir (first args)
          conn (jdbc/get-datasource db-spec)]
      (log/info "Processing TSV files from directory:" dir)
      (process-tsv-files conn dir)
      (log/info "All TSV files have been processed."))))
