/**
 * MyDB client binding for YCSB.
 */

package site.ycsb.db;

import java.io.*;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;


import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * LevelDB client for YCSB framework.
 */
public class MyClient extends DB {

  private static String dbUrl = "http://localhost:8080";
  private static String deleteUrl = dbUrl + "/del";
  private static String insertUrl = dbUrl + "/put";
  private static String readUrl = dbUrl + "/get";
  private static String scanUrl = dbUrl + "/scan";


  private static CloseableHttpClient httpClient;
  private static HttpPost httpPost;
  private static HttpResponse response;
  private static HttpGet httpGet;

  // having multiple tables in leveldb is a hack. must divide key
  // space into logical tables
  private static Map<String, Integer> tableKeyPrefix;


  private static String getStringFromInputStream(InputStream is) {
    BufferedReader br = null;
    StringBuilder sb = new StringBuilder();
    String line;
    try {
      br = new BufferedReader(new InputStreamReader(is));
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return sb.toString();
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    httpClient = HttpClients.createDefault();
    tableKeyPrefix = new HashMap<String, Integer>();
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *            The name of the table
   * @param key
   *            The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *            The name of the table
   * @param key
   *            The record key of the record to insert.
   * @param values
   *            A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
                    Map<String, ByteIterator> values) {
    try {
      String value = null;
      for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
        value=entry.getValue();
      }
      httpGet = new HttpGet(MessageFormat.format(
          "{0}/{1}/{2}", insertUrl, key, value));
      System.out.println("[INSERT] KEY "+key+" VALUE "+value );
      response = httpClient.execute(httpGet);
      EntityUtils.consume(response.getEntity());
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result
   * will be stored in a HashMap.
   *
   * @param table
   *            The name of the table
   * @param key
   *            The record key of the record to read.
   * @param fields
   *            The list of fields to read, or null for all of them
   * @param result
   *            A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      httpGet = new HttpGet(MessageFormat.format("{0}/{1}", readUrl,
          key));

      response = httpClient.execute(httpGet);

      EntityUtils.consume(response.getEntity());
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *            The name of the table
   * @param key
   *            The record key of the record to write.
   * @param values
   *            A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
                    Map<String, ByteIterator> values) {
    try {
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table
   *            The name of the table
   * @param startkey
   *            The record key of the first record to read.
   * @param recordcount
   *            The number of records to read
   * @param fields
   *            The list of fields to read, or null for all of them
   * @param result
   *            A Vector of HashMaps, where each HashMap is a set field/value
   *            pairs for one record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                  Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }
}