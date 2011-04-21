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

(defmulti pack-avro-method class)

(defmethod pack-avro-method :default [obj] obj)

(def pack-avro pack-avro-method)

(defmacro pack-avro-instance
  [type f]
  `(defmethod pack-avro-method ~type [o#]
    (~f o#)))

; Unpacking from Avro object

(defmulti unpack-avro-method (fn [o] (o "_custom_avro_type_")))

(defmethod unpack-avro-method :default [obj] obj)

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

(pack-avro-instance Date
  (fn [#^Date date] (avro-instance avro-date "time" (.getTime date))))
  
(unpack-avro-instance avro-date (fn [rec] (Date. (long (rec "time")))))

; UUID

(defn- uuid-2-bytes
  [#^UUID uuid]
  (let [bb (doto (java.nio.ByteBuffer/wrap (byte-array 16))
             (.putLong (.getMostSignificantBits uuid))
             (.putLong (.getLeastSignificantBits uuid)))]
    (.array bb)))

(defavro-type avro-uuid
   :uuid (avro-fixed "UUID" 16))

(pack-avro-instance UUID
  (fn [#^UUID uuid] (avro-instance avro-uuid "uuid" (uuid-2-bytes uuid))))

(unpack-avro-instance avro-uuid
  #(let [bb    (java.nio.ByteBuffer/wrap (% "uuid"))
         most  (.getLong bb)
         least (.getLong bb)]
     (UUID. most least)))

; Maybe type abstraction for optional values

(defmacro avro-maybe
  [type]
  `(avro-union ~type avro-null))

