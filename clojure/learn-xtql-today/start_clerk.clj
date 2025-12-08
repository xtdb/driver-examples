(require 'nextjournal.clerk)

(nextjournal.clerk/serve! {:watch-paths ["src"]
                           :browse? false
                           :port 7779})

(nextjournal.clerk/show! "src/learn-xtql-today-with-clojure.clj")

(println "Clerk started on port 7779 - open http://localhost:7779")

@(promise)
