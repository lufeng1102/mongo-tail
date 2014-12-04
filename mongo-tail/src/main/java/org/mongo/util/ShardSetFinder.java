package org.mongo.util;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

import org.apache.log4j.Logger;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShardSetFinder {

  private static Logger LOG = Logger.getLogger(ShardSetFinder.class);

  /**
   * connect to mongos and create client for each shard master
   */
  public Map<String, MongoClient> findShardSets(MongoClient mongoS) {
    Map<String, MongoClient> shardSets = new HashMap<String, MongoClient>();
    DBCursor find = null;
    try {
      find = mongoS.getDB("admin").getSisterDB("config")
          .getCollection("shards").find();
      while (find.hasNext()) {
        DBObject next = find.next();
        LOG.info("Adding " + next);
        String key = (String) next.get("_id");
        shardSets.put(key, getMongoClient(buildServerAddressList(next)));
      }
    }finally {
      if(find != null)
        find.close();
    }
    return shardSets;
  }

  public MongoClient getMongoClient(List<ServerAddress> shardSet) {
    MongoClient ShardSetClient = null;
    try {
      MongoClientOptions opts = new MongoClientOptions.Builder()
          .readPreference(ReadPreference.primary()).connectTimeout(10000).socketTimeout(10000).build();
      ShardSetClient = new MongoClient(shardSet, opts);
      Thread.sleep(100); // allow the client to establish prior to being
      // returned for use
    } catch (InterruptedException e) {
      e.printStackTrace();
      LOG.warn("Connect to shard service error. " + e.getMessage());
    }
    return ShardSetClient;
  }

  /**
   * get server address list for each shard
   */
  private List<ServerAddress> buildServerAddressList(DBObject next) {
    List<ServerAddress> hosts = new ArrayList<ServerAddress>();
    for (String host : Arrays
        .asList(((String) next.get("host")).split("/")[1].split(","))) {
      try {
        hosts.add(new ServerAddress(host));
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
    }
    return hosts;
  }
}
