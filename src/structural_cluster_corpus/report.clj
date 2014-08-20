(ns structural-cluster-corpus.report
  "Generate a report on clusters"
  (:require [clojure.java.io :as io]
            [structural-cluster-corpus.format :as format])
  (:use [clojure.pprint :only [pprint]]))

(defn random-take
  ([n coll]
     (random-take n coll []))

  ([n coll taken]
   (cond (empty? coll)
         taken
         
         (zero? n)
         taken

         :else
         (let [chosen (rand-nth coll)
               left (filter
                     #(not= % chosen)
                     coll)]
           (recur (dec n)
                  left
                  (cons chosen taken))))))

(defn cluster-report
  "Args:
   cluster-file: path to a clojure cluster file (ends in .clj)
   warc-file: warc file from which the documents were obtained"
  [cluster-file]
  (let [clusters (read-string
                  (slurp cluster-file))

        num-docs-clustered (count
                            (flatten clusters))
        
        sizes    (map count clusters)
        
        largest (apply max sizes)

        out-file (str cluster-file ".report")]

    (do (with-open [wrtr (io/writer out-file)]
          (pprint
           {:num-clusters (count clusters)
            :num-docs-clustered num-docs-clustered
            :largest-cluster-size largest
            :average-cluster-size (double
                                   (/ (apply + sizes)
                                      (count sizes)))
            :examples-per-cluster (map
                                   (fn [c]
                                     (random-take 10 c))
                                   clusters)}
           wrtr))
        (format/generate-uri-cluster-index cluster-file))))
