ATTACH DATABASE productsdb2 WITH $$
    log: !Kafka
      cluster: "kafkaCluster"
      topic: "productsdb"
      autoCreateTopic: true
    storage: !Remote
      objectStore: !S3
        bucket: "productsdb"
        endpoint: "http://garage:3902"
        pathStyleAccessEnabled: true
        region: "garage"
        credentials: !Basic
          accessKey: "GK31c2f218bd3e1932929759c1"
          secretKey: "b8e1ec4d832d1038fb34242fc0f8e4f1ee8e0ce00fc1be1f12e28550b060c2d5"
$$;
