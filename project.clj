(defproject org.clojars.mjdrogalis/avro-explain "0.1.1"
  :description "Better Avro error messages."
  :url "https://github.com/MichaelDrogalis/avro-explain"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}

  :deploy-repositories [["clojars" {:sign-releases false
                                    :creds :gpg
                                    :url "https://repo.clojars.org/"}]]

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :test-paths ["test/clj"]

  :javac-options ["-target" "1.8" "-source" "1.8"]

  :eftest {:fail-fast? true}

  :dependencies [[org.clojure/clojure "1.11.2"]
                 [org.apache.avro/avro "1.12.0"]
                 [cheshire "5.12.0"]]

  :profiles
  {:dev
   {:repositories {"confluent" "https://packages.confluent.io/maven/"}

    :dependencies [[io.confluent/kafka-avro-serializer "7.9.0"
                    :exclusions [org.apache.avro/avro]]]}})
