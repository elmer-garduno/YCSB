/**
 * Neo4j client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.Vertex;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 * 
 * @author ypai
 * 
 */
public class TitanDbClient extends DB {

  enum Mode {
    EMBEDDED, CASSANDRA
  }

  private static final String NODE_INDEX_NAME = "_id";
  private TitanGraph gds;
  private static Object LOCK = new Object();
  private static TitanGraph GDS;
  private static int instances = 0;
  private Mode mode;

  @Override
  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void init() throws DBException {
    // initialize MongoDb driver
    Properties props = getProperties();
    String url = props.getProperty("titan.url", "data/titan/ycsb");
    String modeName = props.getProperty("titan.mode", "EMBEDDED");
    this.mode = Mode.valueOf(modeName);
    System.out.printf("Running on %s mode\n", mode);
    // database = props.getProperty("neo4j.database", "ycsb");
    try {
      gds = initgdb(url);
      System.out.println("Titan connection created with " + url + "@" + gds);
    } catch (Exception e1) {
      System.err
          .println("Could not initialize titan at host: " + e1.toString());
      e1.printStackTrace();
      return;
    }

  }

  private TitanGraph initgdb(String url) {
    if (mode == Mode.EMBEDDED) {
      synchronized (LOCK) {
        if (GDS == null) {
//          Configuration conf = new BaseConfiguration();
//          conf.setProperty("storage.directory", url);
//          conf.setProperty("storage.transactions", true);
//          conf.setProperty("storage.backend", "local");
          GDS = TitanFactory.open(url);
          GDS.createKeyIndex(NODE_INDEX_NAME, Vertex.class);
        }
        instances++;
        return GDS;
      }
    } else {
      Configuration conf = new BaseConfiguration();
      conf.setProperty("storage.backend","cassandra");
      conf.setProperty("storage.hostname",url);
      TitanGraph gds = TitanFactory.open(conf);
      try {
        gds.createKeyIndex(NODE_INDEX_NAME, Vertex.class);
      } catch (Exception e) {
        System.out.println("Index already exists");
        // Ignore index already exists for this instance
      }
      return gds;
    }
  }

  @Override
  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  public void cleanup() throws DBException {
    try {
      synchronized (LOCK) {
        instances--;
        if (instances == 0) {
          GDS.shutdown();
          System.out.println("titan connection closed with @" + GDS);
        }
      }
    } catch (Exception e1) {
      System.err.println("Could not close Titan connection pool: "
          + e1.toString());
      e1.printStackTrace();
      return;
    }
  }

  private Vertex getVertex(String name, String value) {
    Iterator<Vertex> it = gds.getVertices(name, value).iterator();
    return it.hasNext() ? it.next() : null;
  }

  @Override
  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  public int delete(String table, String key) {
    try {
      Vertex vertex = getVertex("_id", key);
      gds.removeVertex(vertex);
      gds.stopTransaction(Conclusion.SUCCESS);
      return 0;
    } catch (Exception e) {
      gds.stopTransaction(Conclusion.FAILURE);
      e.printStackTrace();
      return 1;
    }
  }

  @Override
  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  public int insert(String table, final String key,
      final HashMap<String, ByteIterator> values) {
    // TitanTransaction tx = gds.startTransaction();
    try {
      Vertex vertex = gds.addVertex(null);
      vertex.setProperty("_id", key);
      for (String k : values.keySet()) {
        vertex.setProperty(k, values.get(k).toArray());
      }
      gds.stopTransaction(Conclusion.SUCCESS);
      return 0;
    } catch (Exception e) {
      gds.stopTransaction(Conclusion.FAILURE);
      e.printStackTrace();
      return 1;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      Vertex vertex = getVertex("_id", key);
      if (vertex != null && fields != null) {
        for (String field : fields) {
          String value = vertex.getProperty(field).toString();
          result.put(field, new StringByteIterator(value));
        }
      }
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }

  @Override
  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  public int update(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      Vertex vertex = gds.getVertices("_id", key).iterator().next();
      for (String k : values.keySet()) {
        vertex.setProperty(k, values.get(k).toArray());
      }
      gds.stopTransaction(Conclusion.SUCCESS);
      return 0;
    } catch (Exception e) {
      gds.stopTransaction(Conclusion.FAILURE);
      e.printStackTrace();
      return 1;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
   */
  public int scan(String table, final String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("SCAN IS NOT YET IMPLEMENTED");
    // try {
    // GremlinPipeline pipe = new GremlinPipeline().property("_id").filter(
    // new PipeFunction<String, Boolean>() {
    // public Boolean compute(String s) {
    // return (s.compareTo(startkey) > 0 && s.compareTo("\uFFFF") < 0);
    // }
    //
    // });
    //
    // for (Iterator<Vertex> hits = pipe.iterator(); hits.hasNext();) {
    // Vertex vertex = hits.next();
    // HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
    // if (fields != null) {
    // for (String field : fields) {
    // String value = vertex.getProperty(field).toString();
    // map.put(field, new StringByteIterator(value));
    // }
    // }
    // result.add(map);
    // }
    // return 0;
    // } catch (Exception e) {
    // e.printStackTrace();
    // return 1;
    // }
  }
}
