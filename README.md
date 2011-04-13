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
(_defavro-record_, _defavro_enum_ and _defavro-fixed) create var objects convenient for hierarchical schema composition.
Parameters namespace, aliases and doc can by provided in an optional argument map.
Use string names for type references to define recursive types, for example:

    (defavro-record IntList
      "value" avro-int 
      "next"  (avro-union "IntList" avro-null))

### Data serialization

    (def contact {:first "Mike" :last ...})
    (def packed (pack Contact contact <optional encoder>))
    (assert (= contact (unpack Contact packed <optional decoder>)))

_pack_ serializes objects into generic Avro objects. For json or binary serialization provide an optional _json-encoder_ or _binary-encoder_.
Use equivalent decoder to de-serialize objects using _unpack_.

For more details, see examples and unit tests.

Found a bug? Have a question? Drop me an email at adam.smyczek \_at\_ gmail.com.

