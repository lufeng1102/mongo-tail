package org.mongo.util;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class PropsLoaderTest {

   @Test
   public void testLoadProps() throws IOException {
      PropsLoader pl = new PropsLoader();
      Properties mongoProps = pl.loadMongoProperties();
      assertEquals("192.168.3.66:27117", mongoProps.get("mongosHostInfo"));
   }

}
