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
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    }

    public SocialNetwork() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        this.tablename = "flefloch";
        if (!admin.tableExists("flefloch")){
            String[] familys = {"info", "friends"};
            createTable(tablename, familys);
        }
    }

    /**
     * Create a table
     */
    public static void createTable(String tableName, String[] familys)
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
    public void addPerson(Person person)
            throws Exception {

        HTable table = new HTable(conf, tablename);

        Put put = new Put(Bytes.toBytes(person.getFirstName()));
        if (person.getLastName() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lastname"), Bytes.toBytes(person.getLastName()));
        if (person.getBirthDate() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthDate"), Bytes.toBytes(person.getBirthDate()));
        if (person.getEmail() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(person.getEmail()));
        if (person.getCity() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(person.getCity()));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("bff"), Bytes.toBytes(person.getBff()));
        if (person.getOtherFriends() != null && !person.getOtherFriends().isEmpty()) {
            for (String friend : person.getOtherFriends())
                put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("others"), Bytes.toBytes(friend));

        }
        table.put(put);
    }

    public void delPerson(String firstname) throws IOException {
        HTable table = new HTable(conf, tablename);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(firstname.getBytes());
        list.add(del);
        table.delete(list);
    }

    public List<String> getPersonAll()throws IOException{
        HTable table = new HTable(conf, tableName);
        List<String> people = new ArrayList<String>();
        Scan s = new Scan();
        ResultScanner ss = table.getScanner(s);
        for (Result rr : ss) {
            people.add(Bytes.toString(rr.getRow());
        }

        return people;

    }

    public Person getPerson(String firstname) throws Exception{
        HTable table = new HTable(conf, tablename);
        Get get = new Get(firstname.getBytes());
        Result rs = table.get(get);
        if (rs.size() == 0){
            throw new Exception();
        }
        Person person = new Person (firstName, null, null, null, null, null, null);
        for(KeyValue kv : rs.raw()){
            switch(kv.getFamily()){
                case "info":
                    switch (kv.getQualifier()){
                        case "lastname": person.setLastName(Bytes.toString(kv.getValue());
                            break;
                        case "email": person.setEmail(Bytes.toString(kv.getValue());
                            break;
                        case "birthDate": person.setBirthDate(Bytes.toString(kv.getValue());
                            break;
                        case "city": person.setCity(Bytes.toString(kv.getValue());
                            break;
                    }
                break;
                case "friends":
                    switch (kv.getQualifier()){
                        case "bff": person.setBff(Bytes.toString(kv.getValue());
                            break;
                        case "others": person.setOtherFriends(Bytes.toString(kv.getValue());
                    }
            }

        }
        return person;

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
