package org.mongo.runner;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ReplicaSetStatus;
import com.mongodb.ServerAddress;

import org.apache.log4j.Logger;
import org.bson.types.BSONTimestamp;
import org.mongo.tail.TailType;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

public class OplogTail implements Runnable {

  private static Logger LOG = Logger.getLogger(OplogTail.class);

  private MongoClient client = null;
  private BSONTimestamp lastTimeStamp = null;
  //private DBCollection shardTimeCollection = null;
  private final List<TailType> tailers;
  private CountDownLatch latch;
  private List<ServerAddress> serverAddresses;
  private TimeDBService timeDBService;
  private String mongoClientKey = null;

  public OplogTail(Entry<String, MongoClient> client, TimeDBService timeDB,
                   List<TailType> tailers, CountDownLatch latch) {
    this.tailers = tailers;
    this.client = client.getValue();
    this.latch = latch;
    this.timeDBService = timeDB;
    this.mongoClientKey = client.getKey();
    this.serverAddresses = this.client.getServerAddressList();
    this.lastTimeStamp = timeDB.getLastTimeStamp(client.getKey());
  }

  @Override
  public void run() {
    DBCollection fromCollection = client.getDB("local").getCollection(
        "oplog.rs");
    DBObject timeQuery = getTimeQuery();
    System.out.println("Start timestamp: " + timeQuery);
    DBCursor opCursor = fromCollection.find(timeQuery)
        .sort(new BasicDBObject("$natural", 1))
        .addOption(Bytes.QUERYOPTION_TAILABLE)
        .addOption(Bytes.QUERYOPTION_AWAITDATA)
        .addOption(Bytes.QUERYOPTION_NOTIMEOUT);
    try {
      while (true) {
        try {
          if (!opCursor.hasNext()) {
            continue;
          } else {
            DBObject nextOp = opCursor.next();
            lastTimeStamp = ((BSONTimestamp) nextOp.get("ts"));
            timeDBService.update(mongoClientKey,lastTimeStamp);
            for (TailType tailer : tailers) {
              tailer.tailOp(nextOp);
            }
          }
        } catch (MongoException e) {
          ReplicaSetStatus replicaSetStatus = client.getReplicaSetStatus();
          ServerAddress masterAddress = replicaSetStatus.getMaster();
          if(masterAddress == null){
            try {
              System.out.println("Master is null and sleep 10s to get new master.");
              Thread.sleep(10000);
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
            // check and retry
            System.out.println("Error: Try again " + e.getMessage());
          }else{
            System.out.println("Now the master is " + masterAddress);
            MongoClientOptions opts = new MongoClientOptions.Builder()
                .readPreference(ReadPreference.primary()).connectTimeout(10000).socketTimeout(10000).build();
            client.close();
            client = new MongoClient(serverAddresses, opts);
            fromCollection = client.getDB("local").getCollection(
                "oplog.rs");
            lastTimeStamp = timeDBService.getLastTimeStamp(mongoClientKey);
            timeQuery = getTimeQuery();
            System.out.println("Start timestamp: " + timeQuery);
            opCursor = fromCollection.find(timeQuery)
                .sort(new BasicDBObject("$natural", 1))
                .addOption(Bytes.QUERYOPTION_TAILABLE)
                .addOption(Bytes.QUERYOPTION_AWAITDATA)
                .addOption(Bytes.QUERYOPTION_NOTIMEOUT);
            try {
              Thread.sleep(100); // allow the client to establish prior to being
            } catch (InterruptedException e1) {
              e1.printStackTrace();
            }
          }

        }
      }
    } finally {
      for (TailType tailer : tailers) {
        tailer.close();
      }
      latch.countDown();
      LOG.info("good bye");
    }
  }

  private DBObject getTimeQuery() {
    return lastTimeStamp == null ? new BasicDBObject() : new BasicDBObject(
        "ts", new BasicDBObject("$gt", lastTimeStamp));
  }

}
