(ns simple-avro.utils
  {:doc "General utils."}
  (:use (simple-avro core))
  (:require (clojure.contrib [json :as json]))
  (:import (org.apache.avro Schema Schema$Type)
           (org.apache.avro.file CodecFactory
                                 DataFileStream
                                 DataFileWriter
                                 DataFileReader)
           (org.apache.avro.io EncoderFactory DecoderFactory)
           (org.apache.avro.generic GenericDatumWriter
                                    GenericDatumReader)
           (java.io InputStream OutputStream File
                    FileOutputStream FileInputStream
                    BufferedOutputStream BufferedInputStream)
           (java.net URI URL)))

;
; Basic read/write avro file functions
;

(defmulti #^{:private true}
  file class)

(defmethod file File [#^File f]
  f)

(defmethod file String [#^String f]
  (File. f))

(defmethod file URL [#^URL f]
  (file (.getPath f)))

(defmethod file URI [#^URI f]
  (file (.toURL f)))

(defn avro-spit
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

(defn avro-slurp
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

(defn avro-slurp-meta
  "Read meta from Avro data file."
  [f]
  (let [reader (DataFileReader. (file f) (GenericDatumReader.))]
    (loop [[k & ks] (.getMetaKeys reader) mta {}]
      (if (nil? k)
        mta
        (recur ks (assoc mta k (String. (.getMeta reader k) "UTF-8")))))))
      
;
; Custom file handling
;

; output stream

(defmulti #^{:private true}
  output-stream class)

(defmethod output-stream OutputStream [#^OutputStream os]
  os)

(defmethod output-stream File [#^File f]
  (BufferedOutputStream. (FileOutputStream. f)))

(defmethod output-stream String [#^String f]
  (output-stream (File. f)))

(defmethod output-stream URL [#^URL f]
  (output-stream (.getPath f)))

(defmethod output-stream URI [#^URI f]
  (output-stream (.toURL f)))

; Writer

(defprotocol Closable
  "Closable protocol."
  (close [this] "Close underlying closable."))

(defprotocol Writer
  "General writer protocol."
  (write        [this obj] "Write object."))

(defn avro-writer
  [os schema]
  (let [schema (avro-schema schema)
        os     (output-stream os) 
        writer (doto (DataFileWriter. (GenericDatumWriter. schema))
                 (.create schema os))]
    (reify
      Writer
      (write [this obj]
             (.append writer (pack schema obj)))
      Closable
      (close [this]
             (.flush writer)
             (.close writer)))))

; Input stream

(defmulti #^{:private true}
  input-stream class)

(defmethod input-stream InputStream [#^OutputStream os]
  os)

(defmethod input-stream File [#^File f]
  (BufferedInputStream. (FileInputStream. f)))

(defmethod input-stream String [#^String f]
  (input-stream (File. f)))

(defmethod input-stream URL [#^URL f]
  (input-stream (.getPath f)))

(defmethod input-stream URI [#^URI f]
  (input-stream (.toURL f)))

(defprotocol Reader
  "General reader protocol."
  (read-next    [this] "Read next element.")
  (has-next     [this] "Checks if more element exists."))

(defn avro-reader
  [is]
  (let [is     (input-stream is)
        dfs    (DataFileStream. is (GenericDatumReader.))
        schema (.getSchema dfs)]
    (reify
      Reader
      (read-next [this] (unpack schema (.next dfs)))
      (has-next  [this] (.hasNext dfs))
      Closable
      (close     [this] (.close dfs)))))
