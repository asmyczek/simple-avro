# simple-avro
Clojure wrapper around Avro schema and serialization.

## Quick Start

### Schema definition

    (defavro-enum State
      "AL" "AK" "AS" "AZ" "AR" "CA" "CO" ; ...
      )

    (defavro-record Address
      :street  avro-string
      :city    avro-string
      :state   State
      :zip     avro-int
      :country avro-string)

    (defavro-record Contact
      :first   avro-string
      :last    avro-string
      :address Address
      :email   avro-string
      :phone   (avro-union avro-string avro-null))

_simple-avro_ implements all types defined in [Avro schema specification](http://avro.apache.org/docs/1.5.0/spec.html).
Just prepend _avro-_ to the type name or use plain string names. _defavro-_ macros defined for all named types
(_defavro-record_, _defavro-enum_ and _defavro-fixed_) create var objects convenient for hierarchical schema compositions.
Parameters _namespace_, _aliases_ and _doc_ can by provided in an optional argument map. In recursive type definitions use 
string names for type references, for example:

    (defavro-record IntList
      :value avro-int 
      :next  (avro-union "IntList" avro-null))

### Data serialization

    (def contact {:first "Mike" :last ...})
    (def packed (pack Contact contact <optional encoder>))
    (assert (= contact (unpack Contact packed <optional decoder>)))

_pack_ serializes objects into generic Avro objects. For json or binary serialization provide an optional _json-encoder_ or _binary-encoder_.
Use equivalent decoder to de-serialize objects using _unpack_.

### Custom types API

_simple-avro.core_ supports only basic Avro types. For custom types import _simple-avro.api_ instead of _core_.
To add support for a custom type first add a schema best matching the type. For example a Date object can be represented as:

    (defavro-type avro-date
      :time avro-long)

Second, register convert functions from the object type to Avro record and back using _pack-avro-instance_ and _unpack-avro-instance_:

    (pack-avro-instance Date
      (fn [date] 
        (avro-instance avro-date "time" (.getTime date))))
      
    (unpack-avro-instance avro-date
      (fn [rec]
        (Date. (rec "time"))))

You can user default pack/unpack method to serialize the Date object now:

    (unpack avro-date (pack avro-date (Date.)))

_simple-avro.api_ adds serialization support for Date, UUID and a _avro-maybe_ helper for optional values.
For more details see examples and unit tests.

## Installation

### Leiningen

    [simple-avro/simple-avro "0.0.2"]
 
### Maven

    <dependency>
      <groupId>simple-avro</groupId>
      <artifactId>simple-avro</artifactId>
      <version>0.0.2</version>
    </dependency>


Found a bug? Have a question? Drop me an email at adam.smyczek \_at\_ gmail.com.

