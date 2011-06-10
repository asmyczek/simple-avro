(ns simple-avro.core
  {:doc "Core namespace defines serialization/de-serialization functions."}
  (:require (clojure.contrib [json :as json]))
  (:import (java.io FileOutputStream ByteArrayOutputStream)
           (org.apache.avro Schema Schema$Type Schema$Field)
           (org.apache.avro.generic GenericData$EnumSymbol
                                    GenericData$Fixed
                                    GenericData$Array
                                    GenericData$Record
                                    GenericDatumWriter
                                    GenericDatumReader)
           (org.apache.avro.io EncoderFactory DecoderFactory)
           (org.apache.avro.util Utf8)))


;
; Encoding 
;

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
  {Schema$Type/NULL     (fn [#^Schema schema obj] nil)
   Schema$Type/BOOLEAN  (fn [#^Schema schema obj] (boolean obj)) 
   Schema$Type/INT      (fn [#^Schema schema obj] (int obj))
   Schema$Type/LONG     (fn [#^Schema schema obj] (long obj))
   Schema$Type/FLOAT    (fn [#^Schema schema obj] (float obj))
   Schema$Type/DOUBLE   (fn [#^Schema schema obj] (double obj))
   Schema$Type/BYTES    (fn [#^Schema schema obj] (bytes obj))

   Schema$Type/STRING   (fn [#^Schema schema obj] (if (string? obj)
                                           (Utf8. (str obj))
                                           (throw (Exception. (str "'" obj "' is not a string.")))))
   Schema$Type/FIXED    (fn [#^Schema schema obj] (GenericData$Fixed. schema obj))

   Schema$Type/ENUM     (fn [#^Schema schema obj] 
                          (if-let [enum (some #{obj} (.getEnumSymbols schema))]
                            (GenericData$EnumSymbol. schema enum)
                            (throw-with-log "Enum does not define '" obj "'.")))

   Schema$Type/UNION    (fn [#^Schema schema obj] 
                          (loop [schemas (.getTypes schema)]
                            (if (empty? schemas)
                              (throw-with-log "No union type defined for object '" obj "'.")
                              (let [rec (try
                                          (pack (first schemas) obj)
                                          (catch Exception e :not-matching-untion-type))] 
                                (if (not= rec :not-matching-untion-type)
                                  rec
                                  (recur (next schemas)))))))

   Schema$Type/ARRAY    (fn [#^Schema schema obj] 
                          (let [type-schema (.getElementType schema)
                                array       (GenericData$Array. (count obj) schema)]
                            (doseq [e obj] (.add array (pack type-schema e)))
                            array))

   Schema$Type/MAP      (fn [#^Schema schema obj] 
                          (let [type-schema (.getValueType schema)]
                            (reduce (fn [m [k v]] (assoc m k (pack type-schema v))) {} obj)))

   Schema$Type/RECORD   (fn [#^Schema schema obj]
                          (if-let [ks (keys obj)]
                            (let [record (GenericData$Record. schema)]
                              (doseq [#^String k ks]
                                (let [field (.getField schema k)]
                                  (.put record k (pack (.schema field) (obj k)))))
                              record)))

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
                 packer (*packers* type)]
    (if packer
      (try
        (encode schema (packer schema obj))
        (catch Exception e
          (throw-with-log "Exception reading object '" obj "' for schema '" schema "'." e)))
      (throw-with-log "No pack defined for type '" type "'."))))

(def json-encoder
  (fn [#^Schema schema obj]
    (encode-to schema obj
      (fn [#^Schema schema #^ByteArrayOutputStream stream]
        (.jsonEncoder (EncoderFactory/get) schema stream))
      (fn [#^ByteArrayOutputStream stream]
        (.toString stream "UTF-8")))))

(def binary-encoder
  (fn [#^Schema schema obj]
    (encode-to schema obj
      (fn [#^Schema schema #^ByteArrayOutputStream stream]
        (.binaryEncoder (EncoderFactory/get) stream nil))
      (fn [#^ByteArrayOutputStream stream]
        (.. stream toString getBytes)))))

;
; Decoding
;

(declare unpack)

(def #^{:private true}
  *unpackers*
  {Schema$Type/NULL     (fn [#^Schema schema obj] nil)
   Schema$Type/BOOLEAN  (fn [#^Schema schema obj] (boolean obj))
   Schema$Type/INT      (fn [#^Schema schema obj] (int obj))
   Schema$Type/LONG     (fn [#^Schema schema obj] (long obj))
   Schema$Type/FLOAT    (fn [#^Schema schema obj] (float obj))
   Schema$Type/DOUBLE   (fn [#^Schema schema obj] (double obj))
   Schema$Type/BYTES    (fn [#^Schema schema obj] (bytes obj))
   Schema$Type/FIXED    (fn [#^Schema schema #^GenericData$Fixed obj] (.bytes obj))
   Schema$Type/ENUM     (fn [#^Schema schema obj] (str obj))

   Schema$Type/STRING   (fn [#^Schema schema obj] (if (instance? Utf8 obj)
                                           (str obj)
                                           (throw (Exception. (str "Object '" obj "' is not a Utf8.")))))

   Schema$Type/UNION    (fn [#^Schema schema obj] 
                          (loop [schemas (.getTypes schema)]
                            (if (empty? schemas)
                              (throw-with-log "No union type defined for object '" obj "'.")
                              (let [rec (try
                                          (unpack (first schemas) obj)
                                          (catch Exception e :not-matching-untion-type))]
                                (if (not= rec :not-matching-untion-type)
                                  rec
                                  (recur (next schemas)))))))

   Schema$Type/ARRAY    (fn [#^Schema schema obj] 
                          (let [type-schema (.getElementType schema)]
                            (vec (map #(unpack type-schema %) obj))))

   Schema$Type/MAP      (fn [#^Schema schema obj] 
                          (let [type-schema (.getValueType schema)]
                            (reduce (fn [m [k v]] (assoc m (str k) (unpack type-schema v))) {} obj)))

   Schema$Type/RECORD   (fn [#^Schema schema #^GenericData$Record obj]
                          (reduce (fn [m #^Schema$Field f]
                                    (let [k (.name f)]
                                      (assoc m k (unpack (.schema f) (.get obj k)))))
                                  {} (.getFields schema)))
    })

(defn- decode-from
  [schema obj decoder]
  (let [reader  (GenericDatumReader. schema)
        decoder (decoder schema obj)]
    (.read reader nil decoder)))

(defn unpack
  [schema obj & [decoder]]
  (let [#^Schema schema   (avro-schema schema)
                 type     (.getType schema)
                 decode   (or decoder (fn [_ obj] obj))
                 unpacker (*unpackers* type)
                 obj      (decode schema obj)]
    (if unpacker
      (try
        (unpacker schema obj)
        (catch Exception e
          (throw-with-log "Exception unpacking object '" obj "' for schema '" schema "'." e)))
      (throw-with-log "No unpack defined for type '" type "'."))))

(def json-decoder
  (fn [#^Schema schema obj]
    (decode-from schema obj
      (fn [#^Schema schema #^String obj]
        (.jsonDecoder (DecoderFactory/get) schema obj)))))

(def binary-decoder
  (fn [#^Schema schema obj]
    (decode-from schema obj
      (fn [#^Schema schema #^bytes obj]
        (.binaryDecoder (DecoderFactory/get) obj nil)))))

; 
; Avro schema generation
;

(def named-types nil)

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

