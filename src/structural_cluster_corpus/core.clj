(ns structural-cluster-corpus.core
  "Structurally cluster a corpus"
  (:require [clojure.java.io :as io]
            [structural-cluster-corpus.cluster :as cluster]
            [subotai.warc.warc :as warc]
            [subotai.structural-similarity.core :as structural-similarity])
  (:use [clojure.pprint :only [pprint]]))

(defn response-html-records
  [records]
  (filter
   (fn [r]
     (and (= (-> r :warc-type) "response")))
   records))

(defn records->corpus
  [records]
  (reduce
   (fn [acc r]
     (merge acc {(:warc-target-uri r)
                 (:payload r)}))
   {}
   records))

(defn handle-warc-file
  [a-warc-file]
  (let [warc-stream (warc/warc-input-stream a-warc-file)
        records (warc/stream-warc-records-seq warc-stream)

        data-records (response-html-records records)

        corpus (records->corpus data-records)

        uris (map first corpus)

        uri-belongs-1 (fn [pt cluster]
                        (some
                         (fn [c-pt]
                           (let [c-body (corpus c-pt)
                                 pt-body (corpus pt)]
                             (structural-similarity/similar? c-body
                                                             pt-body)))
                         cluster))

        uri-belongs-2 (fn [pt cluster]
                        (every?
                         (fn [c-pt]
                           (let [c-body (corpus c-pt)
                                 pt-body (corpus pt)]
                             (structural-similarity/similar? c-body
                                                             pt-body)))
                         cluster))

        uri-belongs-3 (fn [pt cluster]
                        (let [matched (count
                                       (filter
                                        (fn [c-pt]
                                          (let [c-body (corpus c-pt)
                                                pt-body (corpus pt)]
                                            (structural-similarity/similar? c-body
                                                                            pt-body)))
                                        cluster))]
                          (>= matched (/ (count cluster)
                                         2))))]
    (do
      (with-open [wrtr-1 (io/writer (str a-warc-file
                                         ".clusters-single"))
                  wrtr-2 (io/writer (str a-warc-file
                                         ".clusters-all"))
                  wrtr-3 (io/writer (str a-warc-file
                                         ".clusters-majority"))]
        (pprint (cluster/stream-clustering uris
                                           uri-belongs-1)
                wrtr-1)
        (pprint (cluster/stream-clustering uris
                                           uri-belongs-2)
                wrtr-2)
        (pprint (cluster/stream-clustering uris
                                           uri-belongs-3)
                wrtr-3))
      (.close warc-stream))))

(defn -main
  [& args]
  (handle-warc-file
   (first args)))
