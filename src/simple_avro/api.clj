(ns simple-avro.api
  {:doc "Higher level API that supports custom types definition and serialization."}
  (:use (simple-avro schema))
  (:require (simple-avro [core :as core]))
  (:import (java.util Date UUID)))

;
; Custom schema declaration
;

(defmacro defavro-type
  [name & decl]
  `(def ~name
     (avro-record ~(str name)
       ~@decl
       :_custom_avro_type_ avro-string)))

(defmacro avro-instance
  [type & decl]
  `(assoc {"_custom_avro_type_" ~(str type)} ~@decl))

;
; Packing and unpacking for custom types
;

; Packing to Avro object

(defprotocol AvroTypeable 
  "Converts an object to AvroObject wrapper."
  (pack-avro [this]))

(extend-type nil
  AvroTypeable
    (pack-avro [_] nil))

(extend-type Object
  AvroTypeable
    (pack-avro [this] this))

; Unpacking from Avro object

(defmulti unpack-avro-method (fn [o] (o "_custom_avro_type_")))

(defmacro unpack-avro-instance
  [type f]
  `(defmethod unpack-avro-method ~(str type) [o#] (~f o#)))

(defn unpack-avro
  [obj]
  (if-let [t (and (map? obj) (obj "_custom_avro_type_"))]
    (unpack-avro-method obj)
    obj))

;
; Pack and unpack for api objects
; and redirects to public core functions.
;

(defn pack
  [schema obj & [encoder]]
  (let [pa (pack-avro obj)]
    (core/pack schema pa encoder)))

(defn unpack
  [schema obj & [decoder]]
  (let [ua (core/unpack schema obj decoder)]
    (unpack-avro ua)))

(def json-encoder core/json-encoder)
(def json-decoder core/json-decoder)
(def binary-encoder core/binary-encoder)
(def binary-decoder core/binary-decoder)
(def avro-schema core/avro-schema)
(def json-schema core/json-schema)

;
; Common custom types
;

; Date

(defavro-type avro-date
   :time avro-long)

(extend-type Date
  AvroTypeable
    (pack-avro [#^Date this]
      (avro-instance avro-date "time" (.getTime this))))

(unpack-avro-instance avro-date #(Date. (% "time")))

; UUID

(defn- uuid-2-bytes
  [#^UUID uuid]
  (let [bb (doto (java.nio.ByteBuffer/wrap (byte-array 16))
             (.putLong (.getMostSignificantBits uuid))
             (.putLong (.getLeastSignificantBits uuid)))]
    (.array bb)))

(defavro-type avro-uuid
   :uuid (avro-fixed "UUID" 16))

(extend-type UUID
  AvroTypeable
    (pack-avro [#^UUID this]
      (avro-instance avro-uuid "uuid"
        (uuid-2-bytes this))))

(unpack-avro-instance avro-uuid
  #(let [bb    (java.nio.ByteBuffer/wrap (% "uuid"))
         most  (.getLong bb)
         least (.getLong bb)]
     (UUID. most least)))

