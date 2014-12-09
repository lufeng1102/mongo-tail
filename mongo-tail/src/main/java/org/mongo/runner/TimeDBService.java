package org.mongo.runner;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

import org.bson.types.BSONTimestamp;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lemo on 14-12-9.
 */
public class TimeDBService {

  private MongoClient timeClient = null;
  private DB timeDB = null;
  private Map<String,DBCollection> timeCollections = new HashMap<>();
  private List<ServerAddress> hostAddress = null;

  public TimeDBService(String hosts) throws UnknownHostException {
    String[] hostSplits = hosts.split(",");
    hostAddress = new ArrayList<ServerAddress>();
    for(String host : hostSplits)
      hostAddress.add(new ServerAddress(host));
    init();
  }

  public void init(){
    MongoClientOptions opts = new MongoClientOptions.Builder()
        .readPreference(ReadPreference.primary()).connectTimeout(10000).socketTimeout(10000).build();
    if(timeClient != null)
      timeClient.close();
    timeClient = new MongoClient(hostAddress);
    timeDB = timeClient.getDB("time_d");
    timeCollections.clear();
  }

  public void update(String key,BSONTimestamp time){
    DBCollection shardTimeCollection = timeCollections.get(key);
    if(shardTimeCollection == null){
      shardTimeCollection = timeDB.getCollection(key);
      timeCollections.put(key,shardTimeCollection);
    }
    try {
      shardTimeCollection.update(new BasicDBObject(),
                                 new BasicDBObject("$set", new BasicDBObject("ts",
                                                                             time)),
                                 true, true, WriteConcern.SAFE
      );
    }catch (MongoException e) {
      ReplicaSetStatus replicaSetStatus = timeClient.getReplicaSetStatus();
      ServerAddress masterAddress = replicaSetStatus.getMaster();
      if (masterAddress == null) {
        try {
          System.out.println("Master is null and sleep 10s to get new master.");
          Thread.sleep(10000);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        // check and retry
        System.out.println("Error: Try again " + e.getMessage());
      } else {
        System.out.println("Now the master is " + masterAddress);
        init();
        try {
          Thread.sleep(100); // allow the client to establish prior to being
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }
  }

  public void close(){
    if(timeClient != null)
      timeClient.close();
  }

  public BSONTimestamp getLastTimeStamp(String key) {
    DBCollection shardTimeCollection = timeCollections.get(key);
    if(shardTimeCollection == null) {
      shardTimeCollection = timeDB.getCollection(key);
      timeCollections.put(key,shardTimeCollection);
    }
    BSONTimestamp lastTimeStamp = null;
    try {
      DBObject findOne = shardTimeCollection.findOne();
      if (findOne != null) {
        lastTimeStamp = (BSONTimestamp) findOne.get("ts");
      }
    }catch (MongoException e) {
      ReplicaSetStatus replicaSetStatus = timeClient.getReplicaSetStatus();
      ServerAddress masterAddress = replicaSetStatus.getMaster();
      if (masterAddress == null) {
        try {
          System.out.println("Master is null and sleep 10s to get new master.");
          Thread.sleep(10000);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        // check and retry
        System.out.println("Error: Try again " + e.getMessage());
      } else {
        System.out.println("Now the master is " + masterAddress);
        init();
        try {
          Thread.sleep(100); // allow the client to establish prior to being
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }
    return lastTimeStamp;
  }
}
