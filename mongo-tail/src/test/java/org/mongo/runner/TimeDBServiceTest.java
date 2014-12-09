package org.mongo.runner;

import junit.framework.TestCase;

import org.bson.types.BSONTimestamp;
import org.junit.Test;

/**
 * Created by lemo on 14-12-9.
 */
public class TimeDBServiceTest extends TestCase {

  @Test
  public void testUpdate() throws Exception {
    // time
    TimeDBService timeDBService = new TimeDBService("localhost:37017,localhost:37018,localhost:37019");

    while(true){
      Thread.sleep(300);
      System.out.println("Update " + new BSONTimestamp((int)System.currentTimeMillis(),0));
      timeDBService.update("s3",new BSONTimestamp((int)System.currentTimeMillis(),0));
    }
  }
}
