import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class SocialNetwork {

    String tablename;
    String[] familys;

    private static Configuration conf = null;

    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
    }

    public SocialNetwork() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists("flefloch")){
            this.tablename = "flefloch";
            String[] familys = {"info", "friends"};
            creatTable(tablename, familys);
        }
    }

    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public void addPerson(String firstName, String lastName, String birthDate, String email, String city, String bff, List<String> friends)
                            throws Exception {

        try {
            HTable table = new HTable(conf, tablename);
            Put put = new Put(Bytes.toBytes(firstName));
            if (lastName != null)
                put.add(Bytes.toBytes("info"), Bytes.toBytes("lastname"), Bytes.toBytes(lastName));
            if (birthDate != null)
                put.add(Bytes.toBytes("info"), Bytes.toBytes("birthDate"), Bytes.toBytes(birthDate));
            if (email != null)
                put.add(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(email));
            if (city != null)
                put.add(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(city));
            put.add(Bytes.toBytes("friends"), Bytes.toBytes("bff"), Bytes.toBytes(bff));
            if (!friends.isEmpty()) {
                for (String friend : friends)
                    put.add(Bytes.toBytes("friends"), Bytes.toBytes("others"), Bytes.toBytes(friend));
            }
            table.put(put);
            System.out.println("insert recored " + firstName + " to table " + tablename + " ok.");
        } catch (IOException e) {
            System.out.println("Problem occured during the adding");
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for(Result r:ss){
                for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public boolean personExists(String firstname) throws IOException {
        HTable table = new HTable(conf, tablename);
        Get get = new Get(firstname.getBytes());
        Result result = table.get(get);
        if (result.size() == 0){
            return false;
        } else {return true;}
    }


}
