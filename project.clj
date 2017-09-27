(defproject org.onyxplatform/onyx-amazon-kinesis "0.11.0.1-SNAPSHOT"
  :description "Onyx plugin for Amazon Kinesis"
  :url "https://github.com/onyx-platform/onyx-amazon-kinesis"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.11.1-20170927_075340-g7abf65f"]
                 [com.amazonaws/amazon-kinesis-client "1.7.5"]
                 [cheshire "5.7.0"]]
  :profiles {:dev {:dependencies [[aero "0.2.0"]
                                  [prismatic/schema "1.0.5"]] 
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]
                   :global-vars  {*warn-on-reflection* true
                                  *assert* false
                                  *unchecked-math* :warn-on-boxed}
                   :java-opts ^:replace ["-server"
                                         "-XX:+UseG1GC"
                                         "-XX:-OmitStackTraceInFastThrow"
                                         "-Xmx2g"
                                         "-Daeron.client.liveness.timeout=50000000000"
                                         "-XX:+UnlockCommercialFeatures" 
                                         "-XX:+FlightRecorder"
                                         "-XX:+UnlockDiagnosticVMOptions"
                                         "-XX:StartFlightRecording=duration=240s,filename=localrecording.jfr"]}})
