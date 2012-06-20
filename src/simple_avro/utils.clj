(ns simple-avro.utils
  {:doc "General utils."}
  (:use (simple-avro core))
  (:require (clojure.data [json :as json]))
  (:import (org.apache.avro Schema Schema$Type)
           (org.apache.avro.file CodecFactory
                                 DataFileStream
                                 DataFileWriter
                                 DataFileReader
                                 SeekableInput
                                 SeekableFileInput)
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
  (let [#^File   file   (file f)
        #^Schema schema (avro-schema schema)
                 writer (DataFileWriter. (GenericDatumWriter. schema))]
    (try
      (.setCodec writer (CodecFactory/deflateCodec 6))
      (doseq [[k v] meta]
        (.setMeta writer (str k) (str v)))
      (.create writer schema file)
      (doseq [o objs]
        (.append writer (pack schema o)))
      (finally
        (.close writer)))))

(defn avro-slurp
  "Read data from Avro data file."
  [f]
  (let [#^File           file   (file f)
        #^DataFileReader reader (DataFileReader. file (GenericDatumReader.))
        #^Schema         schema (.getSchema reader)
        read-next (fn read-next [#^DataFileReader reader]
                    (lazy-seq 
                      (if (.hasNext reader)
                        (cons (unpack schema (.next reader)) (read-next reader))
                        (do
                          (.close reader)
                          nil))))]
    (read-next reader)))

(defn avro-slurp-meta
  "Read meta from Avro data file."
  [f & keys]
  (let [#^File file (file f)
        reader (DataFileReader. file (GenericDatumReader.))]
    (loop [[k & ks] keys mta {}]
      (if (nil? k)
        mta
        (recur ks (assoc mta k (String. (.getMeta reader k) "UTF-8")))))))
      
;
; Custom file handling
;

; Output stream

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
  (let [#^Schema       schema (avro-schema schema)
        #^OutputStream os     (output-stream os) 
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
  seekable-input class)

(defmethod seekable-input SeekableInput [#^OutputStream si]
  si)

(defmethod seekable-input File [#^File f]
  (SeekableFileInput. f))

(defmethod seekable-input String [#^String f]
  (seekable-input (File. f)))

(defmethod seekable-input URL [#^URL f]
  (seekable-input (.getPath f)))

(defmethod seekable-input URI [#^URI f]
  (seekable-input (.toURL f)))

(defprotocol Reader
  "General reader protocol."
  (read-next [this]     "Read next element.")
  (has-next  [this]     "Checks if more element exists.")
  (sync-pos  [this pos] "Sync reader position.")
  (position  [this]     "Returns approximate position between 0.0 and 1.0."))

(defn avro-reader
  [si & [fields]]
  (let [#^SeekableInput  si     (seekable-input si)
        #^DataFileReader dfr    (DataFileReader. si (GenericDatumReader.))
        #^Schema         schema (.getSchema dfr)]
    (reify
      Reader
      (read-next [this]     (unpack schema (.next dfr) nil fields))
      (has-next  [this]     (.hasNext dfr))
      (sync-pos  [this pos] (.sync dfr pos))
      (position  [this]     (.tell si))
      Closable
      (close     [this]     (.close dfr)))))
