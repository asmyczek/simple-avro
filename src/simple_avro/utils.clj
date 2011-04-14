(ns simple-avro.utils
  {:doc "General utils."}
  (:use (simple-avro core))
  (:require (clojure.contrib [json :as json]))
  (:import (org.apache.avro Schema Schema$Type)
           (org.apache.avro.file CodecFactory
                                 DataFileWriter
                                 DataFileReader)
           (org.apache.avro.generic GenericDatumWriter
                                    GenericDatumReader)
           (java.io File)
           (java.net URI URL)
           (java.util NoSuchElementException)))

(defmulti #^{:private true}
  file class)

(defmethod file File [f]
  f)

(defmethod file String [f]
  (File. f))

(defmethod file URL [f]
  (file (.getPath f)))

(defmethod file URI [f]
  (file (.toURL f)))

(defn write-file
  "Write to Avro data file."
  [f schema objs & [meta]]
  (let [schema (avro-schema schema)
        writer (DataFileWriter. (GenericDatumWriter. schema))]
    (try
      (.setCodec writer (CodecFactory/deflateCodec 6))
      (doseq [[k v] meta]
        (.setMeta writer (str k) (str v)))
      (.create writer schema (file f))
      (doseq [o objs]
        (.append writer (pack schema o)))
      (finally
        (.close writer)))))

(defn read-file
  "Read data from Avro data file."
  [f]
  (let [reader    (DataFileReader. (file f) (GenericDatumReader.))
        schema    (.getSchema reader)
        read-next (fn read-next [reader]
                    (lazy-seq 
                      (if (.hasNext reader)
                        (cons (unpack schema (.next reader)) (read-next reader))
                        (do
                          (.close reader)
                          nil))))]
    (read-next reader)))

(defn read-meta
  "Read meta from Avro data file."
  [f]
  (let [reader (DataFileReader. (file f) (GenericDatumReader.))]
    (loop [[k & ks] (.getMetaKeys reader) mta {}]
      (if (nil? k)
        mta
        (recur ks (assoc mta k (String. (.getMeta reader k) "UTF-8")))))))
      
