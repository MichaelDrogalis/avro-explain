(ns org.clojars.mjdrogalis.avro-explain.core
  (:require [clojure.string :as s]
            [clojure.set :as cset]
            [cheshire.core :as json])
  (:import [org.apache.avro Schema]
           [org.clojars.mjdrogalis.avroexplain Explanation]))

(defn schema->edn [^Schema schema]
  (json/parse-string (.toString schema)))

(def primitive-fns
  {"null" nil?
   "string" string?
   "int" integer?
   "long" integer?
   "boolean" boolean?
   "float" double?
   "double" double?
   "bytes" bytes?})

(defn user-type [x]
  (cond (nil? x) "null"
        (string? x) "string"
        (integer? x) "integer"
        (boolean? x) "boolean"
        (double? x) "double"
        (bytes? x) "bytes"
        :else (type x)))

(defn jmap? [x]
  (instance? java.util.Map x))

(defn jsequential? [x]
  (instance? java.util.List x))

(defn clj->java [x]
  (cond
    (map? x)
    (let [m (java.util.HashMap.)]
      (doseq [[k v] x]
        (.put m (clj->java k) (clj->java v)))
      m)

    (vector? x)
    (let [v (java.util.ArrayList.)]
      (doseq [item x]
        (.add v (clj->java item)))
      v)

    (set? x)
    (let [s (java.util.HashSet.)]
      (doseq [item x]
        (.add s (clj->java item)))
      s)

    (list? x)
    (let [l (java.util.LinkedList.)]
      (doseq [item x]
        (.add l (clj->java item)))
      l)

    (keyword? x) (name x)
    (symbol? x)  (str x)
    :else x))

(defmulti find-error
  (fn [schema data schema-path data-path]
    (cond (jsequential? schema) "union"
          (jmap? schema) (get schema "type")
          :else schema)))

(defmethod find-error "null"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "null")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "null" :actual-type (user-type data)}})))

(defmethod find-error "string"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "string")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "string" :actual-type (user-type data)}})))

(defmethod find-error "int"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "int")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "integer" :actual-type (user-type data)}})))

(defmethod find-error "long"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "long")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "long" :actual-type (user-type data)}})))

(defmethod find-error "boolean"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "boolean")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "boolean" :actual-type (user-type data)}})))

(defmethod find-error "float"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "float")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "float" :actual-type (user-type data)}})))

(defmethod find-error "double"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "double")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "double" :actual-type (user-type data)}})))

(defmethod find-error "bytes"
  [schema data schema-path data-path]
  (let [pred (get primitive-fns "bytes")]
    (when-not (pred data)
      {:schema-path schema-path
       :data-path data-path
       :reason :bad-data-type
       :error-data {:expected-type "bytes" :actual-type (user-type data)}})))

(defmethod find-error "enum"
  [schema data schema-path data-path]
  (when-not (some #{data} (into #{} (get schema "symbols")))
    {:schema-path schema-path
     :data-path data-path
     :reason :bad-enum-symbol
     :error-data {:symbol data}}))

(defmethod find-error "fixed"
  [schema data schema-path data-path]
  (let [size (get schema "size")
        error {:schema-path schema-path
               :data-path data-path
               :reason :bad-fixed-size
               :error-data {:expected-size size}}]
    (cond (and (string? data)
               (not= (count (.getBytes ^String data)) size))
          (assoc-in error [:error-data :actual-size] (count (.getBytes ^String data)))

          (and (bytes? data)
               (not= (count data) size))
          (assoc error [:error-data :actual-size] (count data)))))

(defn rewrite-hint [hint]
  (if (and (jmap? hint) (= (get hint "type") "record"))
    (cond->> (get hint "name")
      (get hint "namespace") (str (get hint "namespace") "."))
    hint))

(defmethod find-error "union"
  [schema data schema-path data-path]
  (if (not (jmap? data))
    {:schema-path schema-path
     :data-path data-path
     :reason :missing-union-hint}
    (let [expected-hints (into #{} schema)
          actual-hints (into #{} (keys data))
          bad-hints (cset/difference actual-hints expected-hints)]
      (if (not (empty? bad-hints))
        {:schema-path schema-path
         :data-path data-path
         :reason :bad-union-type-hint
         :error-data {:expected-hints (into #{} (map rewrite-hint expected-hints))
                      :bad-hints bad-hints}}

        (reduce
         (fn [_ [k i]]
           (when-let [r (find-error k (get data k) (conj schema-path i) (conj data-path k))]
             (reduced r)))
         nil
         (map vector (keys data) (range)))))))

(defmethod find-error "record"
  [schema data schema-path data-path]
  (if (not (jmap? data))
    {:schema-path schema-path
     :data-path data-path
     :reason :bad-record-type}
    (let [expected-fields (into #{} (map (fn [f] (get f "name")) (get schema "fields")))
          actual-fields (into #{} (keys data))]
      (if (every? (fn [f] (some #{f} actual-fields)) expected-fields)
        (reduce
         (fn [_ [field i]]
           (let [field-name (get field "name")]
             (when-let [r (find-error (get field "type") (get data field-name) (into schema-path ["fields" i "type"]) (conj data-path field-name))]
               (reduced r))))
         nil
         (map vector (get schema "fields") (range)))
        (let [missing-field (first (cset/difference expected-fields actual-fields))]
          {:schema-path (conj schema-path "fields")
           :data-path data-path
           :reason :missing-record-field
           :error-data {:missing-field missing-field}})))))

(defmethod find-error "array"
  [schema data schema-path data-path]
  (let [item-schema (get schema "items")
        data (vec data)]
    (reduce
     (fn [_ i]
       (when-let [r (find-error item-schema (get data i) (conj schema-path "items") (conj data-path i))]
         (reduced r)))
     nil
     (range (count data)))))

(defmethod find-error "map"
  [schema data schema-path data-path]
  (if (not (jmap? data))
    {:schema-path schema-path
     :data-path data-path
     :reason :bad-map-type}
    (let [value-schema (get schema "values")]
      (reduce
       (fn [_ [k v]]
         (when-let [r (find-error value-schema v schema-path (conj data-path k))]
           (reduced r)))
       nil
       data))))

(defn reason->msg [reason error-data]
  (cond (= reason :bad-data-type)
        (format "the data type you generated didn't match your schema. Expected type %s, but found type %s." (:expected-type error-data) (:actual-type error-data))

        (= reason :missing-record-field)
        (format "this record is missing required field %s. This StackOverflow answer describes required fields and defaults: https://stackoverflow.com/a/63176493" (:missing-field error-data))

        (= reason :bad-enum-symbol)
        (format "the symbol %s isn't part of your enumeration." (:symbol error-data))

        (= reason :bad-fixed-size)
        (format "generated the wrong number of bytes for this type. Expected %s bytes, but found %s." (:expected-size error-data) (:actual-size error-data))

        (= reason :bad-union-type-hint)
        (format "expected union type hints %s, but found hints %s that don't match." (:expected-hints error-data) (:bad-hints error-data))

        (= reason :bad-record-type)
        "the data you generated wasn't a map type to match the record schema."

        (= reason :bad-map-type)
        "the data you generated wasn't a map type."

        (= reason :missing-union-hint)
        "your schema included a union, but your data didn't include a required hint for its concrete type. This StackOverflow thread describes how to provide the type hint: https://stackoverflow.com/q/27485580"

        :else
        "wasn't able to determine a root cause. This is a bug. Please open a case with ShadowTraffic customer support."))

(defn targeted-error-message [schema data {:keys [schema-path data-path reason error-data]}]
  (let [pretty-schema-path (json/generate-string schema-path {:pretty true})
        pretty-data-path (json/generate-string data-path {:pretty true})

        root-cause (reason->msg reason error-data)

        subschema (get-in schema schema-path)
        subdata (get-in data data-path)]
    (Explanation. root-cause
                  (clj->java subschema)
                  (clj->java schema-path)
                  (clj->java subdata)
                  (clj->java data-path)
                  (name reason)
                  error-data)))

(defn explain [^Schema avro-schema data]
  (when-let [error (find-error (schema->edn avro-schema) data [] [])]
    (targeted-error-message (schema->edn avro-schema) data error)))

(comment

  (set! *warn-on-reflection* true)

  )
