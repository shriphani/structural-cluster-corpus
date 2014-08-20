(ns structural-cluster-corpus.core
  "Structurally cluster a corpus"
  (:require [clojure.java.io :as io]
            [clojure.tools.cli :refer [parse-opts]]
            [structural-cluster-corpus.cluster :as cluster]
            [subotai.warc.warc :as warc]
            [subotai.structural-similarity.core :as structural-similarity]
            [subotai.structural-similarity.xpath-text :as xpath-text]
            [subotai.structural-similarity.edit-distance :as edit-distance])
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
        records (warc/stream-warc-records-seq warc-stream)

        data-records (take 1000 (response-html-records records))]

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
   [nil "--warc-file W" "Warc file to process"]])

(defn -main
  [& args]
  (let [options (:options
                 (parse-opts args cli-options))]
    (with-open [wrtr (io/writer (:out-file options))]
      (cond (and (:max-linkage options)
                 (:xpath-text options))
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :max-linkage
                                   :xpath-text)
             wrtr)

            (and (:single-linkage options)
                 (:xpath-text options))
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :single-linkage
                                   :xpath-text)
             wrtr)

            (and (:max-linkage options)
                 (:edit-distance options))
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :max-linkage
                                   :edit-distance)
             wrtr)

            (and (:single-linkage options)
                 (:edit-distance options))
            (pprint
             (handle-warc-file-100 (:warc-file options)
                                   :single-linkage
                                   :edit-distance)
             wrtr)))))
