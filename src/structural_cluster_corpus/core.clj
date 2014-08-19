(ns structural-cluster-corpus.core
  "Structurally cluster a corpus"
  (:require [clojure.java.io :as io]
            [structural-cluster-corpus.cluster :as cluster]
            [subotai.warc.warc :as warc]
            [subotai.structural-similarity.core :as structural-similarity]
            [subotai.structural-similarity.xpath-text :as xpath-text])
  (:use [clojure.pprint :only [pprint]]
        [subotai.structural-similarity.utils :only [cosine-similarity]]))

(defn response-html-records
  [records]
  (filter
   (fn [r]
     (and (= (-> r :warc-type) "response")
          (re-find #"response" (-> r :content-type))))
   records))

(defn records->corpus-xpaths
  [records]
  (reduce
   (fn [acc r]
     (merge acc {(:warc-target-uri r)
                 (try (->> r
                           :payload
                           xpath-text/page-text-xpaths
                           xpath-text/char-frequency-representation)
                      (catch Exception e nil))}))
   {}
   records))

(defn cluster-single-linkage-xpaths
  [data-records]
  (let [corpus (records->corpus-xpaths data-records)

        uris (map first corpus)
        
        similar? (fn [x y]
                   ;; (println :x x :y y :sim (cosine-similarity (corpus x)
                   ;;                                            (corpus y)))
                   (>= (try (cosine-similarity (corpus x)
                                               (corpus y))
                            (catch Exception e 0))
                       xpath-text/*sim-thresh*))

        belongs? (fn [pt cluster]
                   (some
                    #(similar? % pt)
                    cluster))]      
    (cluster/stream-clustering uris
                               belongs?)))

(defn cluster-max-linkage-xpaths
  [data-records]
  (let [corpus (records->corpus-xpaths data-records)

        uris (map first corpus)
        
        similar? (fn [x y]
                   ;; (println :x x :y y :sim (cosine-similarity (corpus x)
                   ;;                                            (corpus y)))
                   (>= (try (cosine-similarity (corpus x)
                                               (corpus y))
                            (catch Exception e 0))
                       xpath-text/*sim-thresh*))

        belongs? (fn [pt cluster]
                   (every?
                    #(similar? % pt)
                    cluster))]      
    (cluster/stream-clustering-max-linkage uris
                                           belongs?)))

(defn handle-warc-file-100
  [a-warc-file]
  (let [warc-stream (warc/warc-input-stream a-warc-file)
        records (warc/stream-warc-records-seq warc-stream)

        data-records (take 1000 (response-html-records records))]
    (reverse
     (sort-by
      count
      (cluster-max-linkage-xpaths data-records)))))

(defn -main
  [& args]
  (with-open [wrtr (io/writer (second args))]
    (pprint
     (handle-warc-file-100
      (first args))
     wrtr)))
