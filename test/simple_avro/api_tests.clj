(ns simple-avro.api-tests
  (:use (simple-avro core schema api)
        (clojure test))
  (:import (java.util Date UUID)))

(deftest test-prim-types
  (is (= (pack avro-null    nil)          nil))
  (is (= (pack avro-null    5)            nil))
  (is (= (pack avro-boolean true)         true))
  (is (= (pack avro-boolean nil)          false))
  (is (= (pack avro-int     5)            5))
  (is (= (pack avro-long    10)           (long 10)))
  (is (= (pack avro-long    (long 10))    (long 10)))
  (is (= (pack avro-float   2.5)          (float 2.5)))
  (is (= (pack avro-float   (float 2.5))  (float 2.5)))
  (is (= (pack avro-double  2.5)          (double 2.5)))
  (is (= (pack avro-double  (double 2.5)) (double 2.5)))
  (is (= (str (pack avro-string  "test")) "test")))

; Some types
(def bool-array (avro-array avro-boolean))
(def int-map    (avro-map avro-int))
(def a-union    (avro-union avro-string avro-int avro-null))

(defavro-fixed MyFixed 2)

(defavro-enum MyEnum "A" "B" "C")

(defavro-record MyRecord
  "f1" avro-int
  "f2" avro-string)

(defavro-record MyNestedRecord
  "f1" avro-int
  "f2" avro-string)

(defavro-record List
  "value" avro-int 
  "next"  (avro-union "List" avro-null))

(def map-in-map 
  {"value" 1 
   "next"  {"value" 2
            "next"  {"value" 3
                     "next"  nil}}})

(def maybe-date
  (avro-maybe avroDate))

(defavro-record DateRecord
  "date" avroDate)

(defmacro test-pack-unpack
  [name encoder decoder]
  `(deftest ~name
    (is (= (unpack avro-null    (pack avro-null    nil  ~encoder) ~decoder)         nil))
    (is (= (unpack avro-null    (pack avro-null    5    ~encoder) ~decoder)         nil))
    (is (= (unpack avro-boolean (pack avro-boolean true ~encoder) ~decoder)         true))
    (is (= (unpack avro-int     (pack avro-int     5    ~encoder) ~decoder)         5))
    (is (= (unpack avro-long    (pack avro-long    10   ~encoder) ~decoder)         (long 10)))
    (is (= (unpack avro-float   (pack avro-float   2.5  ~encoder) ~decoder)         (float 2.5)))
    (is (= (unpack avro-double  (pack avro-double  2.5  ~encoder) ~decoder)         (double 2.5)))
    (is (= (str (unpack avro-string (pack avro-string  "test" ~encoder) ~decoder))  "test"))

    (is (= (unpack bool-array (pack bool-array [true false false] ~encoder) ~decoder) [true false false]))
    (is (= (unpack int-map (pack int-map {"a" 1 "b" 2} ~encoder) ~decoder) {"a" 1 "b" 2}))

    (is (= (unpack a-union (pack a-union "test" ~encoder) ~decoder) "test"))
    (is (= (unpack a-union (pack a-union 10 ~encoder) ~decoder) 10))

    (let [pu# (unpack MyFixed (pack MyFixed (byte-array [(byte 1) (byte 2)]) ~encoder) ~decoder)]
      (is (= (nth pu# 0) 1))
      (is (= (nth pu# 1) 2)))

    (is (= (unpack MyEnum (pack MyEnum "A" ~encoder) ~decoder) "A"))
    (is (= (unpack MyEnum (pack MyEnum "B" ~encoder) ~decoder) "B"))
    (is (= (unpack MyEnum (pack MyEnum "C" ~encoder) ~decoder) "C"))

    (let [pu# (unpack MyRecord (pack MyRecord {"f1" 6 "f2" "test"} ~encoder) ~decoder)]
      (is (= (pu# "f1") 6))
      (is (= (pu# "f2") "test")))

    (is (= (unpack List (pack List map-in-map ~encoder) ~decoder) map-in-map))

    (let [now# (Date.)]
      (is (= (unpack avroDate (pack avroDate now# ~encoder) ~decoder) now#))
      (is (= (unpack maybe-date (pack maybe-date now# ~encoder) ~decoder) now#))
      (is (= (unpack maybe-date (pack maybe-date nil ~encoder) ~decoder) nil)))

    (let [now-record# {"date" (Date.)}]
      (is (= (unpack DateRecord (pack DateRecord now-record# ~encoder) ~decoder) now-record#)))
    
    (let [uuid# (UUID/randomUUID)]
      (is (= (unpack avroUUID (pack avroUUID uuid# ~encoder) ~decoder) uuid#)))

  ))

(pack DateRecord {"date" (Date.)} json-encoder)


(test-pack-unpack test-prim-types-pack-unpack-no-decoder nil nil)
(test-pack-unpack test-prim-types-pack-unpack-json json-encoder json-decoder)
(test-pack-unpack test-prim-types-pack-unpack-binary binary-encoder binary-decoder)
