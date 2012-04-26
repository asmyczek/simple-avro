(ns simple-avro.avro-date-tests
  (:use (simple-avro api schema utils)
        (clojure test)))

(defavro-record Test
  "field1" avro-string
  "field2" avro-int
  "field3" avro-date
  )

(def test-records
  [{"field1" "record1" "field2" 10 "field3" (java.util.Date. )}
   {"field1" "record2" "field2" 20 "field3" (java.util.Date. )}
   {"field1" "record3" "field2" 30 "field3" (java.util.Date. )}
   {"field1" "record4" "field2" 40 "field3" (java.util.Date. )}])


;
; Reader/writer example
;

(defn write-read-data
  []
  (let [tmp-file (.getAbsolutePath (java.io.File/createTempFile "test" ".avro"))]
    (let [writer   (avro-writer tmp-file Test)]
      (println "Creating temp file " tmp-file)
      (doseq [rec (apply concat (repeat 1000 test-records))]
        (write writer rec))
      (close writer))
    (println "Reading " tmp-file)
    (let [reader (avro-reader tmp-file)]
      (loop [nx (has-next reader)]
        (when nx
          (println "Read     " (read-next reader))
          (println "Position " (position reader))
          (recur (has-next reader))))
      (close reader))
    (println "Deleting " tmp-file)
    (.delete (java.io.File. tmp-file))))

(deftest writer-reader-test  (write-read-data))
