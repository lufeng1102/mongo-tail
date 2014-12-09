package org.mongo.runner;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

import junit.framework.TestCase;

import org.junit.Test;
import org.mongo.tail.TailType;
import org.mongo.tail.types.NoopTailType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by lemo on 14-12-9.
 */
public class OplogTailTest extends TestCase {

  @Test
  public void testRun() throws Exception {
    //public OplogTail(Entry<String, MongoClient > client, DB timeDB,
    //                 List< TailType > tailers) {
    List<TailType> tailers = new ArrayList<TailType>();
    tailers.add(new NoopTailType());

    //
    List<ServerAddress> shardSet = new ArrayList<ServerAddress>();
    shardSet.add(new ServerAddress("localhost:37017"));
    shardSet.add(new ServerAddress("localhost:37018"));
    shardSet.add(new ServerAddress("localhost:37019"));
    MongoClientOptions opts = new MongoClientOptions.Builder()
        .readPreference(ReadPreference.primary()).connectTimeout(10000).socketTimeout(10000).build();
    MongoClient ShardSetClient = new MongoClient(shardSet, opts);
    //
    HashMap<String,MongoClient> clients = new HashMap<>();
    clients.put("s3",ShardSetClient);
    // time
    TimeDBService timeDBService = new TimeDBService("localhost:37020");
    //
    ExecutorService executor = Executors.newFixedThreadPool(clients.size());
    System.out.println("Size:"+clients.size());
    CountDownLatch latch = new CountDownLatch(clients.size());
    for (Map.Entry<String, MongoClient> client : clients.entrySet()) {
      Runnable worker = new OplogTail(client, timeDBService, tailers,latch);
      executor.execute(worker);
    }
    executor.shutdown();
    try {
      latch.await();
    } catch (InterruptedException E) {
      // handle
    }
    System.out.println("good bye");
  }
}
