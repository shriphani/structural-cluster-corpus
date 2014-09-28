(ns structural-cluster-corpus.core
  "Structurally cluster a corpus"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [structural-cluster-corpus.cluster :as cluster]
            [structural-cluster-corpus.report :as report]
            [subotai.warc.warc :as warc]
            [subotai.structural-similarity.core :as structural-similarity]
            [subotai.structural-similarity.xpath-text :as xpath-text]
            [subotai.structural-similarity.edit-distance :as edit-distance])
  (:use [clojure.pprint :only [pprint]]
        [subotai.structural-similarity.utils :only [cosine-similarity]]))

(defn html-records
  "Accept response records
   filtered out by Subotai and
   retrieve HTML"
  [response-records]
  (map
   (fn [r]
     (let [html-starts (.indexOf (:payload r)
                                 "<")]
       (merge r {:payload (if (neg? html-starts)
                            (:payload r)
                            (subs (:payload r)
                                  html-starts))})))
   response-records))

(defn records->corpus-xpaths
  "Scale the score by the XPath DF"
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

(defn records->corpus-xpaths-scale
  "Scale the score by the XPath DF"
  [records]
  (let [no-idf-map (records->corpus-xpaths records)

        df-components (reduce
                       (fn [dfs [uri paths-scored]]
                         (merge-with
                          +
                          dfs
                          (reduce
                           (fn [dfs-inner [path _]]
                             (merge-with + dfs-inner {path 1}))
                           {}
                           paths-scored)))
                       {}
                       no-idf-map)]
    (reduce
     (fn [acc [uri paths-scored]]
       (merge
        acc
        {uri (into
              {}
              (map
               (fn [[path tf]]
                 [path (/ tf (df-components path))])
               paths-scored))}))
     {}
     no-idf-map)))

(defn records->corpus-xpaths-idf
  "Scale the score by the IDF term"
  [records]
  (let [no-idf-map (records->corpus-xpaths records)

        df-components (reduce
                       (fn [dfs [uri paths-scored]]
                         (merge-with
                          +
                          dfs
                          (reduce
                           (fn [dfs-inner [path _]]
                             (merge-with + dfs-inner {path 1}))
                           {}
                           paths-scored)))
                       {}
                       no-idf-map)

        N (count records)]
    (reduce
     (fn [acc [uri paths-scored]]
       (merge
        acc
        {uri (into
              {}
              (map
               (fn [[path tf]]
                 [path (* tf (Math/log
                              (/ N
                                 (df-components path))))])
               paths-scored))}))
     {}
     no-idf-map)))


(defn records->corpus-tree
  [records]
  (reduce
   (fn [acc r]
     (merge acc {(:warc-target-uri r)
                 (try (->> r
                           :payload
                           edit-distance/html->map)
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
  (let [corpus (records->corpus-xpaths-idf data-records)
        
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

(defn cluster-max-linkage-edit
  [data-records]
  (let [corpus (records->corpus-tree data-records)

        uris (map first corpus)

        similar? (fn [x y]
                   ;; (println :x x :y y :sim (cosine-similarity (corpus x)
                   ;;                                            (corpus y)))
                   (>= (try (- 1
                               (/ (edit-distance/tree-edit-distance (corpus x)
                                                                    (corpus y)
                                                                    1
                                                                    1
                                                                    1)
                                  (+ (edit-distance/tree-descendants (corpus x))
                                     (edit-distance/tree-descendants (corpus y)))))
                            (catch Exception e 0))
                       edit-distance/*sim-thresh*))

        belongs? (fn [pt cluster]
                   (every?
                    #(similar? % pt)
                    cluster))]
    (cluster/stream-clustering-max-linkage uris
                                           belongs?)))

(defn cluster-single-linkage-edit
  [data-records]
  (let [corpus (records->corpus-tree data-records)

        uris (map first corpus)

        similar? (fn [x y]
                   ;; (println :x x :y y :sim (cosine-similarity (corpus x)
                   ;;                                            (corpus y)))
                   (>= (try (- 1
                               (/ (edit-distance/tree-edit-distance (corpus x)
                                                                    (corpus y)
                                                                    1
                                                                    1
                                                                    1)
                                  (+ (edit-distance/tree-descendants (corpus x))
                                     (edit-distance/tree-descendants (corpus y)))))
                            (catch Exception e 0))
                       edit-distance/*sim-thresh*))

        belongs? (fn [pt cluster]
                   (some
                    #(similar? % pt)
                    cluster))]
    (cluster/stream-clustering uris
                               belongs?)))

(defn handle-warc-file-100
  [a-warc-file linkage algorithm]
  (let [warc-stream (warc/warc-input-stream a-warc-file)
        records
        (filter
         (fn [r]
           (re-find #"HTTP.*200"
                    (:payload r)))
         (warc/stream-html-records-seq warc-stream))

        data-records (take 1000 (html-records records))]

    (cond (and (= linkage :max-linkage)
               (= algorithm :edit-distance))
          (reverse
           (sort-by
            count
            (cluster-max-linkage-edit data-records)))

          (and (= linkage :single-linkage)
               (= algorithm :edit-distance))
          (reverse
           (sort-by
            count
            (cluster-single-linkage-edit data-records)))

          (and (= linkage :max-linkage)
               (= algorithm :xpath-text))
          (reverse
           (sort-by
            count
            (cluster-max-linkage-xpaths data-records)))

          (and (= linkage :single-linkage)
               (= algorithm :xpath-text))
          (reverse
           (sort-by
            count
            (cluster-single-linkage-xpaths data-records))))))

(def cli-options
  [[nil "--max-linkage" "Use max linkage"]
   [nil "--edit-distance" "Use edit distance"]
   [nil "--xpath-text" "Use xpath text"]
   [nil "--single-linkage" "Use single linkage"]
   [nil "--out-file F" "Write clusters to file"]
   [nil "--warc-file W" "Warc file to process"]
   [nil "--report C" "Cluster file to report on"]])

(defn -main
  [& args]
  (let [options (:options
                 (parse-opts args cli-options))]
    (cond (and (:max-linkage options)
               (:xpath-text options))
          (with-open [wrtr (io/writer (:out-file options))]
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :max-linkage
                                   :xpath-text)
             wrtr))

          (and (:single-linkage options)
               (:xpath-text options))
          (with-open [wrtr (io/writer (:out-file options))]
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :single-linkage
                                   :xpath-text)
             wrtr))

          (and (:max-linkage options)
               (:edit-distance options))
          (with-open [wrtr (io/writer (:out-file options))]
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :max-linkage
                                   :edit-distance)
             wrtr))

          (and (:single-linkage options)
               (:edit-distance options))
          (with-open [wrtr (io/writer (:out-file options))]
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :single-linkage
                                   :edit-distance)
             wrtr))

          (:report options)
          (report/cluster-report (:report options)))))
