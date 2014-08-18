(ns structural-cluster-corpus.cluster
  "Clustering algorithms")

(defn stream-clustering
  [points belongs?]
  (reduce
   (fn [clusters pt]
     (let [similar (first
                    (filter
                     (fn [c]
                       (belongs? pt c))
                     clusters))

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
   points))
