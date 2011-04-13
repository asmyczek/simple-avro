(ns examples.address-book
  (:use (simple-avro schema core)))

; Schema

(defavro-enum Country
  "USA" "Germany" "France" ; ...
  )

(defavro-enum State
  "AL" "AK" "AS" "AZ" "AR" "CA" "CO" ; ...
  )

(defavro-record Address
  :street  avro-string
  :city    avro-string
  :state   State
  :zip     avro-int
  :country Country)

(defavro-record Person
  :first   avro-string
  :last    avro-string
  :address Address
  :email   avro-string
  :phone   (avro-union avro-string avro-null))

(defavro-record Company
  :name    avro-string
  :address Address
  :contact Person)

(def Contact
  (avro-union Person Company))

(def AddressBook
  (avro-array Contact))


; Sample records

(def address-book
  [{"first"  "Mike"
   "last"    "Foster"
   "address" {"street"  "South Park Str. 14"
              "city"    "Wasco"
              "state"   "CA"
              "zip"     95171
              "country" "USA"}
   "email"   "mike@home.com"
   "phone"   nil}])

; Serialization

(let [packed-address-book   (pack AddressBook address-book)
      unpacked-address-book (unpack AddressBook packed-address-book)]
  (assert (= address-book unpacked-address-book)))

