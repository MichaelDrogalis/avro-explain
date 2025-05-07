(ns org.clojars.mjdrogalis.avro-explain.core-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json])
  (:import [org.apache.avro Schema]
           [org.clojars.mjdrogalis.avroexplain ExplainAvro]
           [org.clojars.mjdrogalis.avroexplain Explanation]
           [io.confluent.kafka.schemaregistry.avro AvroSchema]))

(defn ^Schema make-schema [schema]
  (.rawSchema (AvroSchema. (json/generate-string schema))))

(defn exp->map [^Explanation explanation]
  {:schema-path (.getSchemaPath explanation)
   :data-path (.getDataPath explanation)
   :reason (.getReason explanation)
   :error-data (.getErrorData explanation)})

(deftest no-errors
  (testing "primitives"
    (let [schema (make-schema "int")
          data 42]
      (is (nil? (ExplainAvro/explain schema data))))))

(deftest primitives
  (testing "null"
    (let [schema (make-schema "null")
          data true]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "null" :actual-type "boolean"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "string"
    (let [schema (make-schema "string")
          data 42]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "string" :actual-type "integer"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "int"
    (let [schema (make-schema "int")
          data "abc"]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "integer" :actual-type "string"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "long"
    (let [schema (make-schema "long")
          data 42.4]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "long" :actual-type "double"}}
             (exp->map (ExplainAvro/explain schema data))))))
  
  (testing "boolean"
    (let [schema (make-schema "boolean")
          data "def"]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "boolean" :actual-type "string"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "float"
    (let [schema (make-schema "float")
          data true]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "float" :actual-type "boolean"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "double"
    (let [schema (make-schema "double")
          data "abc"]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "double" :actual-type "string"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "bytes"
    (let [schema (make-schema "bytes")
          data true]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-data-type"
              :error-data {:expected-type "bytes" :actual-type "boolean"}}
             (exp->map (ExplainAvro/explain schema data)))))))

(deftest unions
  (testing "no type hint"
    (let [schema (make-schema ["string" "boolean"])
          data "abc"]
      (is (= {:schema-path []
              :data-path []
              :reason "missing-union-hint"
              :error-data nil}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "invalid type hint"
    (let [schema (make-schema ["string" "boolean"])
          data {"float" "abc"}]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-union-type-hint"
              :error-data {:expected-hints #{"boolean" "string"}
                           :bad-hints #{"float"}}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "bad type"
    (let [schema (make-schema ["string" "boolean"])
          data {"string" 42.0}]
      (is (= {:schema-path [0]
              :data-path ["string"]
              :reason "bad-data-type"
              :error-data {:expected-type "string" :actual-type "double"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "bad type + no hint"
    (let [schema (make-schema ["string" "boolean"])
          data 42.0]
      (is (= {:schema-path []
              :data-path []
              :reason "missing-union-hint"
              :error-data nil}
             (exp->map (ExplainAvro/explain schema data)))))))

(deftest records
  (testing "missing field"
    (let [raw-schema {"type" "record"
                      "name" "a"
                      "fields" [{"name" "b" "type" "string"}
                                {"name" "c" "type" "float"}]}
          schema (make-schema raw-schema)
          data {"b" "x"}]
      (is (= {:schema-path ["fields"]
              :data-path []
              :reason "missing-record-field"
              :error-data {:missing-field "c"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "mistyped field"
    (let [raw-schema {"type" "record"
                      "name" "a"
                      "fields" [{"name" "b" "type" "string"}]}
          schema (make-schema raw-schema)
          data {"b" 42}]
      (is (= {:schema-path ["fields" 0 "type"]
              :data-path ["b"]
              :reason "bad-data-type"
              :error-data {:expected-type "string" :actual-type "integer"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "no union type hint"
    (let [schema (make-schema {"type" "record"
                               "name" "a"
                               "fields" [{"name" "b"
                                          "type" ["string" "float"]}]})
          data {"b" 42}]
      (is (= {:schema-path ["fields" 0 "type"]
              :data-path ["b"]
              :reason "missing-union-hint"
              :error-data nil}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "unusable type hint"
    (let [schema (make-schema {"type" "record"
                               "name" "a"
                               "fields" [{"name" "b"
                                          "type" ["string" "float"]}]})
          data {"b" {"boolean" true}}]
      (is (= {:schema-path ["fields" 0 "type"]
              :data-path ["b"]
              :reason "bad-union-type-hint"
              :error-data {:expected-hints #{"float" "string"}
                           :bad-hints #{"boolean"}}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "nested records"
    (let [raw-schema {"type" "record"
                      "name" "a"
                      "namespace" "x"
                      "fields" [{"name" "b" "type" "string"}
                                {"name" "c"
                                 "type" ["null"
                                         {"type" "record"
                                          "namespace" "y"
                                          "name" "d"
                                          "fields" [{"name" "e" "type" "string"}
                                                    {"name" "f" "type" "string"}]}]
                                 "default" nil}]}
          schema (make-schema raw-schema)
          data {"b" "foo"
                "c" {"string" {"e" "bar"
                               "f" 0}}}]
      (is (= {:schema-path ["fields" 1 "type"]
              :data-path ["c"]
              :reason "bad-union-type-hint"
              :error-data {:expected-hints #{"y.d" "null"}
                           :bad-hints #{"string"}}}
             (exp->map (ExplainAvro/explain schema data)))))))

(deftest misc-types
  (testing "enums"
    (let [raw-schema {"type" "enum" "name" "foo" "symbols" ["a" "b"]}
          schema (make-schema raw-schema)
          data "q"]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-enum-symbol"
              :error-data {:symbol "q"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "arrays"
    (let [schema (make-schema {"type" "record"
                               "name" "a"
                               "fields" [{"name" "b" "type" {"type" "array" "items" "string"}}]})
          data {"b" ["x" 2 "z"]}]
      (is (= {:schema-path ["fields" 0 "type" "items"]
              :data-path ["b" 1]
              :reason "bad-data-type"
              :error-data {:expected-type "string" :actual-type "integer"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "maps"
    (let [raw-schema {"type" "map" "values" "int"}
          schema (make-schema raw-schema)
          data {"b" true "c" 1}]
      (is (= {:schema-path []
              :data-path ["b"]
              :reason "bad-data-type"
              :error-data {:expected-type "integer" :actual-type "boolean"}}
             (exp->map (ExplainAvro/explain schema data))))))

  (testing "fixed"
    (let [raw-schema {"type" "fixed" "size" 16 "name" "x"}
          schema (make-schema raw-schema)
          data "aaaaaaaaaaaaaaaaa"]
      (is (= {:schema-path []
              :data-path []
              :reason "bad-fixed-size"
              :error-data {:expected-size 16 :actual-size 17}}
             (exp->map (ExplainAvro/explain schema data)))))))
