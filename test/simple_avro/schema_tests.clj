(ns simple-avro.schema-tests
  (:use (simple-avro schema core)
        (clojure test)))

(deftest test-prim-types
  (is (= avro-null    {:type "null"}))
  (is (= avro-boolean {:type "boolean"}))
  (is (= avro-int     {:type "int"}))
  (is (= avro-long    {:type "long"}))
  (is (= avro-float   {:type "float"}))
  (is (= avro-double  {:type "double"}))
  (is (= avro-bytes   {:type "bytes"}))
  (is (= avro-string  {:type "string"}))) 

(deftest test-complex-types
  (is (= (avro-array avro-int) {:type "array" :items {:type "int"}}))
  (is (= (avro-map avro-string) {:type "map" :values {:type "string"}}))
  (is (= (avro-union avro-string avro-int avro-null)
         [{:type "string"} {:type "int"} {:type "null"}])))

(deftest test-named-types
  (is (= (avro-fixed "MyFixed" 16)
         {:type "fixed" :size 16 :name "MyFixed"}))
  (is (= (avro-enum "MyEnum" "A" "B" "C")
         {:type "enum" :symbols ["A" "B" "C"] :name "MyEnum"}))
  (is (= (avro-record "MyRecord"
            "f1" avro-int
            "f2" avro-string)
         {:type "record"
          :name "MyRecord"
          :fields [{:name "f1" :type {:type "int"}}
                   {:name "f2" :type {:type "string"}}]})))

(defavro-fixed MyDefFixed 16)

(defavro-enum MyDefEnum "A" "B" "C")

(defavro-record MyDefRecord
  "f1" avro-int
  "f2" avro-string)

(deftest test-defavro
  (is (= MyDefFixed {:type "fixed" :size 16 :name "MyDefFixed"}))
  (is (= MyDefEnum {:type "enum" :symbols ["A" "B" "C"] :name "MyDefEnum"}))
  (is (= MyDefRecord
         {:type "record"
          :name "MyDefRecord"
          :fields [{:name "f1" :type {:type "int"}}
                   {:name "f2" :type {:type "string"}}]})))


(deftest test-opts
  (is (= (avro-fixed "MyFixed" 16 {:namespace "test-namespace"})
         {:type "fixed" :size 16 :name "MyFixed" :namespace "test-namespace"}))
  (is (= (avro-enum "MyEnum" {:namespace "test-namespace"} "A" "B" "C")
         {:type "enum" :symbols ["A" "B" "C"] :name "MyEnum" :namespace "test-namespace"}))
  (is (= (avro-record "MyRecord" {:namespace "test-namespace"}
            "f1" avro-int
            "f2" avro-string)
         {:type "record"
          :name "MyRecord"
          :namespace "test-namespace"
          :fields [{:name "f1" :type {:type "int"}}
                   {:name "f2" :type {:type "string"}}]})))

