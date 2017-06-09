package hbaseproject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
	
	/**Create a table
	 * 
	 * @param tableName
	 * @param familys
	 * @param conf
	 * @throws Exception
	 */
    public static void creatTable(TableName tableName, String[] familys, Configuration conf) throws Exception {
//      HBaseAdmin admin = new HBaseAdmin(conf);    	
    	HBaseAdmin admin = (HBaseAdmin) ConnectionFactory.createConnection(conf).getAdmin();
    
    	if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName);
        }
        admin.close();
    }
 
    /**
     * Delete a table	
     */
    public static void deleteTable(TableName tableName, Configuration conf) throws Exception {
        try {
        	//HBaseAdmin admin = new HBaseAdmin(conf);
        	HBaseAdmin admin = (HBaseAdmin) ConnectionFactory.createConnection(conf).getAdmin();
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Delete table " + tableName);
            admin.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Put (or insert) a row
     */
    public static void addRecord(TableName tableName, String rowKey, String family, String qualifier, String value, Configuration conf) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            table.close();	
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
 
    /**
     * Delete a row
     */
    public static void delRecord(TableName tableName, String rowKey, Configuration conf)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);       
        table.close();
    }
 
        
    public static void getRecord (TableName tableName, String rowKey, Configuration conf) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
        	System.out.println(displayRecord(kv));   
        }
        table.close();
    }
    /**
     * List table
     */    
	public static void getAllRecord (TableName	tableName, Configuration conf) {
        try {
             HTable table = new HTable(conf, tableName);
             Scan s = new Scan();
             ResultScanner ss = table.getScanner(s);
             for(Result r : ss){
                 for(KeyValue kv : r.raw()){                	
                	System.out.println(displayRecord(kv));
                 }
             }
             table.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
	
	private static String displayRecord(KeyValue kv) {
    	StringBuffer sb = new StringBuffer();
    	sb.append(new String(kv.getRow())).append(" ")
    	.append(new String(kv.getFamily())).append(":")
        .append(new String(kv.getQualifier())).append(" ")         
        .append(new String(kv.getValue()));
    	
    	return sb.toString();

	}
}
	