(require '[clojure.java.io :as io]
         '[cognitect.transit :as transit])

(defn convert-transit-json-to-msgpack []
  (let [input-file "sample-users-transit.json"
        output-file "sample-users-transit.msgpack"]
    (println (str "Reading from: " input-file))
    (println (str "Writing to: " output-file))

    ;; Read all transit-json records and convert to msgpack
    ;; Convert _id to xt/id for XTDB COPY compatibility
    (with-open [in (io/reader input-file)]
      (let [records (doall
                     (for [line (line-seq in)
                           :when (not (empty? (clojure.string/trim line)))]
                       (let [bytes (.getBytes line "UTF-8")
                             in-stream (java.io.ByteArrayInputStream. bytes)
                             json-reader (transit/reader in-stream :json)]
                         ;; No conversion needed - transit-json now uses string keys "_id" and "_valid_from"
                         ;; which XTDB accepts directly
                         (transit/read json-reader))))]
        ;; Write all records to msgpack using serde (same as XTDB tests)
        (with-open [out (io/output-stream output-file)]
          (let [msgpack-writer (transit/writer out :msgpack)]
            (doseq [record records]
              (transit/write msgpack-writer record))))
        (println (str "Converted " (count records) " records to transit-msgpack successfully!"))))))

(convert-transit-json-to-msgpack)
