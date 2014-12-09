package org.mongo.runner;

import com.mongodb.MongoClient;

import org.apache.log4j.Logger;
import org.mongo.tail.TailType;
import org.mongo.tail.TailTypeInjector;
import org.mongo.util.PropsLoader;
import org.mongo.util.ShardSetFinder;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ShardedReplicaTailer {
  private static MongoClient hostMongoS = null;
  private static Map<String, MongoClient> shardSetClients;
  private static TimeDBService timeDB;
  private static ExecutorService executor;
  private static CountDownLatch latch;

  private static Logger LOG = Logger.getLogger(ShardedReplicaTailer.class);

  public static void main(String[] args) throws UnknownHostException {
    LOG.info("Beginning ShardedReplicaTailer: "
             + Arrays.asList(args));
    try {
      addShutdownHookToMainThread();
      establishMongoDBConnections();
      runTailingThreads(args.length > 0 ? args : new String[]{""});
      try {
        latch.await();
      } catch (InterruptedException E) {
        // handle
      }
    } finally {
      if (executor != null) {
        executor.shutdownNow();
      }
      closeMongoConnections();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  private static void establishMongoDBConnections()
      throws UnknownHostException {
    Properties mongoConnectionProperties = loadProperties();
    hostMongoS = new MongoClient(
        mongoConnectionProperties.getProperty("mongosHostInfo"));
    timeDB = new TimeDBService(mongoConnectionProperties.getProperty("mongoReplTimeHostInfo"));
    shardSetClients = new ShardSetFinder().findShardSets(hostMongoS);
  }

  private static void runTailingThreads(String... tailTypes) {
    LOG.info("Beginning tailable mongo using: "+ Arrays.asList(tailTypes));
    executor = Executors.newFixedThreadPool(shardSetClients.size());
    latch = new CountDownLatch(shardSetClients.size());
    for (Entry<String, MongoClient> client : shardSetClients.entrySet()) {
      Runnable worker = new OplogTail(client, timeDB, getOpType(tailTypes),latch);
      executor.execute(worker);
    }
    executor.shutdown();
  }

  private static List<TailType> getOpType(String... tailTypes) {
    return new TailTypeInjector().getTailTypeFromArgs(tailTypes);
  }

  private static void closeMongoConnections() {
    if (hostMongoS != null) {
      hostMongoS.close();
    }
    if (timeDB != null) {
      timeDB.close();
    }
    if (shardSetClients != null) {
      for (MongoClient repClient : shardSetClients.values()) {
        repClient.close();
      }
    }
  }

  private static Properties loadProperties() {
    PropsLoader propsLoader = new PropsLoader();
    return propsLoader.loadMongoProperties();
  }

  public static void addShutdownHookToMainThread() {
    final Thread mainThread = Thread.currentThread();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        LOG.info("Closing MongoDB connections through shutdown hook");
        closeMongoConnections();
        try {
          mainThread.join();
        } catch (InterruptedException e) {
          LOG.info(
                  "---------------- Unable to join main thread, attempting to shutdown MongoDB connections gracefully. --------------");
          closeMongoConnections();
          throw new RuntimeException(e);
        }
      }
    });
  }

}
