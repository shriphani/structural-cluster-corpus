(ns structural-cluster-corpus.cluster
  "Clustering algorithms")

(defn stream-clustering-iterate
  [clusters merge?]
  (let [new-clusters
        (reduce
         (fn [clusters-so-far cluster]
           (let [to-merge (first
                           (filter
                            (fn [cluster-seen]
                              (merge? cluster-seen cluster))
                            clusters-so-far))

                 to-merge-index (if (nil? to-merge)
                                  -1
                                  (.indexOf clusters-so-far to-merge))]
             (if (neg? to-merge-index)
               (into [] (cons cluster clusters-so-far))
               (assoc clusters-so-far
                 to-merge-index
                 (concat cluster
                         (get clusters to-merge-index))))))
         []
         clusters)]
    (println :iterating!)
    (if (= (count new-clusters)
           (count clusters))
      new-clusters
      (recur new-clusters merge?))))

(defn stream-clustering
  [points belongs?]
  (let [initialized
        (reduce
         (fn [clusters pt]
           (let [sorted-clusters (reverse
                                  (sort-by
                                   #(-> % count)
                                   clusters))
                 
                 similar (first
                          (filter
                           (fn [c]
                             (belongs? pt c))
                           sorted-clusters))

                 cluster-index (if (nil? similar)
                                 -1
                                 (.indexOf clusters similar))]
             (if (neg? cluster-index)
               (into [] (cons [pt] clusters))
               (assoc clusters
                 cluster-index
                 (concat [pt]
                         (get clusters cluster-index))))))
         []
         points)]
    (stream-clustering-iterate initialized
                               (fn [c1 c2]
                                 (some
                                  (fn [p]
                                    (belongs? p c2))
                                  c1)))))

(defn stream-clustering-max-linkage
  [points belongs?]
  (let [initialized
        (reduce
         (fn [clusters pt]
           (let [sorted-clusters (reverse
                                  (sort-by
                                   #(-> % count)
                                   clusters))
                 
                 similar (first
                          (filter
                           (fn [c]
                             (belongs? pt c))
                           sorted-clusters))

                 cluster-index (if (nil? similar)
                                 -1
                                 (.indexOf clusters similar))]
             (if (neg? cluster-index)
               (into [] (cons [pt] clusters))
               (assoc clusters
                 cluster-index
                 (concat [pt]
                         (get clusters cluster-index))))))
         []
         points)]
    (stream-clustering-iterate initialized
                               (fn [c1 c2]
                                 (every?
                                  (fn [p]
                                    (belongs? p c2))
                                  c1)))))

(defn better-stream-clustering
  [points similar?]
  (let [initial-assignments (reduce
                             (fn [[assignments next-cluster] pt]
                               (println pt)
                               (let [assign-attempt (first
                                                     (filter
                                                      (fn [[c-pt i]]
                                                        (similar? c-pt pt))
                                                      assignments))]
                                 (if-not (nil? assign-attempt)
                                   [(merge assignments {pt (second assign-attempt)}) next-cluster]
                                   [(merge assignments {pt next-cluster}) (inc next-cluster)])))
                             [{} 0]
                             points)]
    (first initial-assignments)))
