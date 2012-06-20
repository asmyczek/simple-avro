(ns simple-avro.core
  {:doc "Core namespace defines serialization/de-serialization functions."}
  (:require (clojure.data [json :as json]))
  (:import (java.io FileOutputStream ByteArrayOutputStream ByteArrayInputStream)
           (org.apache.avro Schema Schema$Type Schema$Field)
           (org.apache.avro.generic GenericData$EnumSymbol
                                    GenericData$Fixed
                                    GenericData$Array
                                    GenericData$Record
                                    GenericDatumWriter
                                    GenericDatumReader)
           (org.apache.avro.io DecoderFactory EncoderFactory)
           (org.apache.avro.util Utf8)))

;
; Encoding 
;

; Typabe protocol to convert custom types to Avro compatible format,
; currenty only record supported (see api.clj).
(defprotocol AvroTypeable
  (avro-pack [this] "Pack custom type into avro."))

; Default implementations
(extend-type Object
  AvroTypeable
    (avro-pack [this] this))
    
(extend-type nil
  AvroTypeable
    (avro-pack [_] nil))

(declare pack avro-schema)

(defmacro throw-with-log
  "Throw exception and log helper."
  {:private true}
  [ & msg ]
  `(let [l#  ~(last msg)]
     (if (instance? Exception l#)
       (throw (Exception. (apply str ~(vec (butlast msg))) l#))
       (throw (Exception. (str ~@msg))))))

(def #^{:private true}
  *packers*
  {Schema$Type/NULL     (fn pack-nil     [#^Schema schema obj] nil)
   Schema$Type/BOOLEAN  (fn pack-boolean [#^Schema schema obj] (boolean obj)) 
   Schema$Type/INT      (fn pack-int     [#^Schema schema obj] (int obj))
   Schema$Type/LONG     (fn pack-long    [#^Schema schema obj] (long obj))
   Schema$Type/FLOAT    (fn pack-float   [#^Schema schema obj] (float obj))
   Schema$Type/DOUBLE   (fn pack-double  [#^Schema schema obj] (double obj))
   Schema$Type/BYTES    (fn pack-bytes   [#^Schema schema obj] (bytes obj))

   Schema$Type/STRING   (fn pack-string  [#^Schema schema obj] (if (string? obj)
                                           (Utf8. (str obj))
                                           (throw (Exception. (str "'" obj "' is not a string.")))))
   Schema$Type/FIXED    (fn pack-fixed [#^Schema schema obj] (doto (GenericData$Fixed. schema)
                                                    (.bytes obj)))

   Schema$Type/ENUM     (fn pack-enum [#^Schema schema obj] 
                          (if-let [enum (some #{obj} (.getEnumSymbols schema))]
                            (GenericData$EnumSymbol. schema enum)
                            (throw-with-log "Enum does not define '" obj "'.")))

   Schema$Type/UNION    (fn pack-union [#^Schema schema obj] 
                          (loop [schemas (seq (.getTypes schema))]
                            (if (empty? schemas)
                              (throw-with-log "No union type defined for object '" obj "'.")
                              (let [rec (try
                                          (pack (first schemas) obj)
                                          (catch Exception e :not-matching-untion-type))] 
                                (if (not= rec :not-matching-untion-type)
                                  rec
                                  (recur (next schemas)))))))

   Schema$Type/ARRAY    (fn pack-array [#^Schema schema obj] 
                          (let [type-schema (.getElementType schema)
                                array       (GenericData$Array. (count obj) schema)]
                            (doseq [e obj] (.add array (pack type-schema e)))
                            array))

   Schema$Type/MAP      (fn pack-map [#^Schema schema obj] 
                          (let [type-schema (.getValueType schema)]
                            (reduce (fn [m [k v]] (assoc m k (pack type-schema v))) {} obj)))

   Schema$Type/RECORD   (fn pack-record [#^Schema schema obj]
                          (if-let [ks (keys obj)]
                            (try
                            (let [record (GenericData$Record. schema)]
                              (doseq [#^String k ks]
                                (let [field (.getField schema k)]
                                  (when (nil? field)
                                    (throw (Exception. (str "Null field " k " schema " schema))))
                                  (.put record k (pack (.schema field) (obj k)))))
                              record)
                              (catch Exception e
                                (throw (Exception. (str ">>> " schema " - " obj) e))))))

    })

(defn- encode-to
  [#^Schema schema obj encoder result]
  (let [stream  (ByteArrayOutputStream.)
        writer  (GenericDatumWriter. schema)
        #^java.io.Flushable encoder (encoder schema stream)]
    (.write writer obj encoder)
    (.flush encoder)
    (result stream)))

(defn pack
  [schema obj & [encoder]]
  (let [#^Schema schema (avro-schema schema)
                 type   (.getType schema)
                 encode (or encoder (fn [_ obj] obj))
                 packer (*packers* type)
                 obj    (avro-pack obj)]
    (if packer
      (try
        (encode schema (packer schema obj))
        (catch Exception e
          (throw-with-log "Exception reading object '" obj "' for schema '" schema "'." e)))
      (throw-with-log "No pack defined for type '" type "'."))))

(def json-encoder
  (fn json-encoder-fn [#^Schema schema obj]
    (encode-to schema obj
      (fn [#^Schema schema #^ByteArrayOutputStream stream]
        (.jsonEncoder (EncoderFactory/get) schema stream))
      (fn [#^ByteArrayOutputStream stream]
        (.toString stream "UTF-8")))))

(def binary-encoder
  (fn binary-encoder-fn [#^Schema schema obj]
    (encode-to schema obj
      (fn [#^Schema schema #^ByteArrayOutputStream stream]
        (.binaryEncoder (EncoderFactory/get) stream nil))
      (fn [#^ByteArrayOutputStream stream]
        (.. stream toByteArray)))))

;
; Decoding
;

; Unpack multi method
(defmulti unpack-avro (fn [schema obj] (.getName schema)))

(defmethod unpack-avro :default [schema obj] obj)

(declare unpack-impl json-schema)

(defn- unpack-record-fields
  "Unpack only provided fields from record object."
  [#^Schema schema #^GenericData$Record obj fields rmap]
  (loop [[f & fs] fields m rmap]
    (if f
      (if (coll? f)
        (if-let [#^Schema$Field fd (.getField schema (name (first f)))]
          (let [k (.name fd)]
            (if (next f)
              (recur fs (assoc m k (unpack-record-fields (.schema fd) (.get obj k) (rest f) (m k))))
              (recur fs (assoc m k (unpack-impl (.schema fd) (.get obj k))))))
          (throw-with-log "No field for name '" (first f) "' exists in schema " (json-schema schema)))
        (if-let [#^Schema$Field fd (.getField schema (name f))]
          (let [k (.name fd)]
            (recur fs (assoc m k (unpack-impl (.schema fd) (.get obj k)))))
          (throw-with-log "No field for name '" f "' exists in schema " (json-schema schema))))
      m)))

(defn- unpack-all-record-fields
  "Unpack entire record object."
  [#^Schema schema #^GenericData$Record obj]
  (persistent! (reduce (fn [m #^Schema$Field f]
            (let [k (.name f)]
              (assoc! m k (unpack-impl (.schema f) (.get obj k)))))
          (transient {}) (.getFields schema))))

(def #^{:private true}
  *unpackers*
  {Schema$Type/NULL     (fn unpack-nil     [#^Schema schema obj] (if obj (throw (Exception. "Nil expected."))))
   Schema$Type/BOOLEAN  (fn unpack-boolean [#^Schema schema obj] (boolean obj))
   Schema$Type/INT      (fn unpack-int     [#^Schema schema obj] (int obj))
   Schema$Type/LONG     (fn unpack-long    [#^Schema schema obj] (long obj))
   Schema$Type/FLOAT    (fn unpack-float   [#^Schema schema obj] (float obj))
   Schema$Type/DOUBLE   (fn unpack-double  [#^Schema schema obj] (double obj))
   Schema$Type/BYTES    (fn unpack-bytes   [#^Schema schema obj] (bytes obj))
   Schema$Type/FIXED    (fn unpack-fixed   [#^Schema schema #^GenericData$Fixed obj] (.bytes obj))
   Schema$Type/ENUM     (fn unpack-enum    [#^Schema schema obj] (str obj))

   Schema$Type/STRING   (fn unpack-stirng  [#^Schema schema obj] (if (instance? Utf8 obj)
                                           (str obj)
                                           (throw (Exception. (str "Object '" obj "' is not a Utf8.")))))

   Schema$Type/UNION    (fn unpack-union   [#^Schema schema obj] 
                          (loop [schemas (.getTypes schema)]
                            (if (empty? schemas)
                              (throw-with-log "No union type defined for object '" obj "'.")
                              (let [rec (try
                                          (unpack-impl (first schemas) obj)
                                          (catch Exception e :not-matching-untion-type))]
                                (if (not= rec :not-matching-untion-type)
                                  rec
                                  (recur (next schemas)))))))

   Schema$Type/ARRAY    (fn unpack-array [#^Schema schema obj] 
                          (let [type-schema (.getElementType schema)]
                            (vec (map #(unpack-impl type-schema %) obj))))

   Schema$Type/MAP      (fn unpack-map [#^Schema schema obj] 
                          (let [type-schema (.getValueType schema)]
                            (reduce (fn [m [k v]] (assoc m (str k) (unpack-impl type-schema v))) {} obj)))

   Schema$Type/RECORD   (fn unpack-record [#^Schema schema #^GenericData$Record obj fields]
                          (if (empty? fields)
                            (unpack-all-record-fields schema obj)
                            (unpack-record-fields schema obj fields {})))

    })

(defn- decode-from
  [schema obj decoder]
  (let [reader  (GenericDatumReader. schema)
        decoder (decoder schema obj)]
    (.read reader nil decoder)))

(defn- unpack-impl
  ([#^Schema schema obj]
    (unpack-impl schema obj nil))
  ([#^Schema schema obj fields]
    (let [type     (.getType schema)
          unpacker (*unpackers* type)]
      (if unpacker
        (let [obj (if (= type Schema$Type/RECORD)
                    (unpacker schema obj fields)
                    (unpacker schema obj))]
          (unpack-avro schema obj))
        (throw-with-log "No unpack defined for type '" type "'.")))))

(defn unpack
  ([schema obj]
    (unpack schema obj nil nil))
  ([schema obj decoder & [fields]]
  (let [#^Schema schema   (avro-schema schema)
                 decode   (or decoder (fn [_ obj] obj))
                 obj      (decode schema obj)]
    (try
      (unpack-impl schema obj fields)
      (catch Exception e
        (throw-with-log "Exception unpacking object '" obj "' for schema '" schema "'." e))))))

(def json-decoder
  (fn json-decoder-fn [#^Schema schema obj]
    (decode-from schema obj
      (fn decode-from-json [#^Schema schema #^String obj]
        (let [is (ByteArrayInputStream. (.getBytes obj "UTF-8"))]
          (.jsonDecoder (DecoderFactory/get) schema obj))))))

(def binary-decoder
  (let [factory (DecoderFactory/defaultFactory)
        decode-from-binary (fn decode-from-binary
                             [#^Schema schema #^bytes obj]
                             (.binaryDecoder factory obj nil))]
    (fn binary-decoder-fn [#^Schema schema obj]
      (decode-from schema obj decode-from-binary))))

;
; Custom avro type methods
;

(defmacro def-unpack-avro
  [type f]
  `(defmethod unpack-avro ~(str type) [s# o#] (~f o#)))

; 
; Avro schema generation
;

(def ^:dynamic named-types nil)

(defn- traverse-schema
  "Traverse types of a schema."
  [schema f]
  (cond 
    (vector? schema)
      (vec (map #(f %) schema))

    (map? schema)
      (case (:type schema)
        "array"   (assoc schema :items (f (:items schema)))
        "map"     (assoc schema :values (f (:values schema)))
        "record"  (assoc schema :fields (vec (map #(assoc % :type (f (:type %))) (:fields schema))))
        schema)
    :else schema))

(defn- flatten-named-types
  "Ensures a named type is only defined once."
  [schema]
  (if-let [name (:name schema)]
    (if (some @named-types [name])
      name
      (let [schema (traverse-schema schema flatten-named-types)]
        (swap! named-types conj name)
        schema))
    (traverse-schema schema flatten-named-types)))
        
(defn avro-schema
  "Convert a simple-avro or json string schema to Avro schema object."
  [schema]
  (cond 
    (instance? Schema schema)
      schema
    (string? schema)
      (Schema/parse #^String schema)
    :else
      (let [schema (binding [named-types (atom #{})]
                     (flatten-named-types schema))]
        (Schema/parse #^String (json/json-str schema)))))

(defn json-schema
  "Print schema to a json string. Provide optional parameter {:pretty bool}
  for pretty printing. Default is false."
  [schema & [opts]]
  (let [pretty (or (:pretty opts) false)]
    (.toString #^Schema (avro-schema schema) pretty)))

