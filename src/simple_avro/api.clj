(ns simple-avro.api
  {:doc "Higher level API that supports custom types definition and serialization."}
  (:use (simple-avro core schema))
  (:import (java.util Date UUID)))

;
; Packing and unpacking for custom types
;

;
; Common custom types
;

; Date

(defavro-record AvroDate
   :time avro-long)

(extend-type Date
  AvroTypeable
    (avro-pack [date] {"time" (.getTime date)}))

(def-unpack-avro AvroDate (fn [rec] (Date. (long (rec "time")))))

; UUID

(defn- uuid-2-bytes
  [#^UUID uuid]
  (let [bb (doto (java.nio.ByteBuffer/wrap (byte-array 16))
             (.putLong (.getMostSignificantBits uuid))
             (.putLong (.getLeastSignificantBits uuid)))]
    (.array bb)))

(defavro-record avroUUID
   :uuid (avro-fixed "UUID" 16))

(extend-type UUID
  AvroTypeable
    (avro-pack [uuid] {"uuid" (uuid-2-bytes uuid)}))

(def-unpack-avro avroUUID
  #(let [bb    (java.nio.ByteBuffer/wrap (% "uuid"))
         most  (.getLong bb)
         least (.getLong bb)]
     (UUID. most least)))

; Maybe type abstraction for optional values

(defmacro avro-maybe
  [type]
  `(avro-union ~type avro-null))

