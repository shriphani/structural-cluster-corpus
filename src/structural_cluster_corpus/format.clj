(ns structural-cluster-corpus.format
  "Takes your emitted clusters (which are pprinted clj data structures)
   and produces a variety of outputs"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]))

(defn generate-uri-cluster-index
  [file]
  (let [clusters (map
                  vector
                  (read-string
                   (slurp file))
                  (iterate inc 0))

        out-file (string/replace file
                                 #".clj$"
                                 ".csv")]
    (with-open [wrtr (io/writer out-file)]
      (doseq [[c i] clusters]
        (doseq [uri c]
          (binding [*out* wrtr]
            (println uri "," i)))))))
