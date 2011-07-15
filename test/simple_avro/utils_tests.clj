(ns simple-avro.utils-tests
  (:use (simple-avro schema core utils)
        (clojure test)))

(defavro-record Test
  "field1" avro-string
  "field2" avro-int)

(def test-records
  [{"field1" "record1" "field2" 10}
   {"field1" "record2" "field2" 20}
   {"field1" "record3" "field2" 30}
   {"field1" "record4" "field2" 40}])

(deftest read-write-test
  (let [file    (java.io.File/createTempFile "avro-test-data", ".tmp")
        _       (avro-spit file Test test-records {"m1" "test1" "m2" "test2"})
        content (avro-slurp file)
        meta    (avro-slurp-meta file)]
    (is (= content test-records))
    (is (= (meta "m1") "test1"))
    (is (= (meta "m2") "test2"))))

