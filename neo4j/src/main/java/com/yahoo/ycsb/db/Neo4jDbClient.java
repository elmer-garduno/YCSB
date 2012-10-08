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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.rest.graphdb.RestAPI;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.rest.graphdb.batch.BatchCallback;
import org.neo4j.rest.graphdb.index.RestIndex;

import com.google.common.collect.ImmutableMap;
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
public class Neo4jDbClient extends DB {

  enum Mode {
    BATCH, EMBEDDED, REST
  }

  private static final String NODE_INDEX_NAME = "node_index";
  private GraphDatabaseService gds;
  private static Object LOCK = new Object();
  private static GraphDatabaseService GDS;
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
    String url = props.getProperty("neo4j.url", "data/ycsb");
    String modeName = props.getProperty("neo4j.mode", "EMBEDDED");
    this.mode = Mode.valueOf(modeName);
    System.out.printf("Running on %s mode\n", mode);
    // database = props.getProperty("neo4j.database", "ycsb");
    try {
      gds = initgdb(url);
      System.out.println("neo4j connection created with " + url + "@" + gds);
    } catch (Exception e1) {
      System.err
          .println("Could not initialize neo4j at host: " + e1.toString());
      e1.printStackTrace();
      return;
    }

  }

  private GraphDatabaseService initgdb(String url) {
    if (mode == Mode.EMBEDDED) {
      synchronized (LOCK) {
        if (GDS == null) {
          GDS = new EmbeddedGraphDatabase(url);
        }
        instances++;
        return GDS;
      }
    } else {
      return new RestGraphDatabase(url + "/db/data/");
    }
  }

  private Index<Node> index() {
    return gds.index().forNodes(NODE_INDEX_NAME);
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
          System.out.println("neo4j connection closed with @" + GDS);
        }
      }
    } catch (Exception e1) {
      System.err.println("Could not close MongoDB connection pool: "
          + e1.toString());
      e1.printStackTrace();
      return;
    }
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
    Transaction tx = gds.beginTx();
    try {
      IndexHits<Node> hits = index().get("_id", key);
      Node node = hits.getSingle();
      index().remove(node);
      node.delete();
      tx.success();
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    } finally {
      tx.finish();
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
    try {
      if (mode == Mode.BATCH) {
        doBatchInsert(table, key, values);
      } else {
        doInsert(table, key, values);
      }
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }

  public void doInsert(String table, final String key,
      final HashMap<String, ByteIterator> values) {
    Transaction tx = gds.beginTx();
    try {
      Node node = gds.createNode();
      node.setProperty("_id", key);
      for (String k : values.keySet()) {
        node.setProperty(k, values.get(k).toArray());
      }
      index().add(node, "_id", key);
      tx.success();
    } finally {
      tx.finish();
    }
  }

  public void doBatchInsert(String table, final String key,
      final HashMap<String, ByteIterator> values) {
    RestAPI restAPI = ((RestGraphDatabase) gds).getRestAPI();
    restAPI.executeBatch(new BatchCallback<Object>() {
      @Override
      public Object recordBatch(RestAPI batchRestApi) {
        Map<String, Object> map = ImmutableMap.of("_id", (Object) key);
        Node node = batchRestApi.createNode(map);
        RestIndex<Node> index = batchRestApi.getIndex(NODE_INDEX_NAME);
        batchRestApi.addToIndex(node, index, "_id", key);
        for (String k : values.keySet()) {
          node.setProperty(k, values.get(k).toArray());
        }
        return new Object();
      }
    });
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
      IndexHits<Node> hits = index().get("_id", key);
      Node node = hits.getSingle();
      if (fields != null) {
        for (String field : fields) {
          String value = node.getProperty(field).toString();
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
    Transaction tx = gds.beginTx();
    try {
      IndexHits<Node> hits = index().get("_id", key);
      Node node = hits.getSingle();
      for (String k : values.keySet()) {
        node.setProperty(k, values.get(k).toArray());
      }
      index().remove(node, "_id", key);
      index().add(node, "_id", key);
      tx.success();
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    } finally {
      tx.finish();
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
  public int scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      QueryContext ctx = new QueryContext(String.format("_id:[%s TO \uFFFF*]",
          startkey));
      IndexHits<Node> hits = index().query(ctx);
      for (Node node : hits) {
        HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
        if (fields != null) {
          for (String field : fields) {
            String value = node.getProperty(field).toString();
            map.put(field, new StringByteIterator(value));
          }
        }
        result.add(map);
      }
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }
}
