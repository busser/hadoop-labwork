import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class SocialNetwork {

    String tableName;

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
        tableName = "flefloch";
        if (!admin.tableExists("flefloch")){
            String[] families = {"info", "friends"};
            createTable(tableName, families);
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

        HTable table = new HTable(conf, Bytes.toBytes(tableName));

        Put put = new Put(Bytes.toBytes(person.getFirstName()));
        if (person.getLastName() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lastname"), Bytes.toBytes(person.getLastName()));
        if (person.getBirthDate() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthdate"), Bytes.toBytes(person.getBirthDate()));
        if (person.getEmail() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(person.getEmail()));
        if (person.getCity() != null)
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(person.getCity()));
        put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("bff"), Bytes.toBytes(person.getBff()));
        if (person.getOtherFriends() != null && !person.getOtherFriends().isEmpty()) {
            put.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("others"), Bytes.toBytes(StringUtils.join(person.getOtherFriends(), ",")));
        }
        table.put(put);
    }

    public void deletePerson(String firstname) throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(firstname.getBytes());
        list.add(del);
        table.delete(list);
    }

    public List<Person> getPersonAll()throws IOException{
        HTable table = new HTable(conf, tableName);
        List<Person> people = new ArrayList<Person>();
        Scan s = new Scan();
        ResultScanner ss = table.getScanner(s);
        for (Result rr : ss) {
            try {
                people.add(getPerson(Bytes.toString(rr.getRow())));
            } catch (Exception e) {
                // Should never happen
            }
        }
        return people;
    }

    public Person getPerson(String firstName) throws Exception{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(firstName.getBytes());
        Result rs = table.get(get);
        if (rs.size() == 0){
            throw new Exception();
        }
        Person person = new Person (firstName, null, null, null, null, null, null);
        for(KeyValue kv : rs.raw()){
            if (Bytes.toString(kv.getFamily()).equals("info")) {
                if (Bytes.toString(kv.getQualifier()).equals("lastname")) {
                    person.setLastName(Bytes.toString(kv.getValue()));
                } else if (Bytes.toString(kv.getQualifier()).equals("email")) {
                    person.setEmail(Bytes.toString(kv.getValue()));
                } else if (Bytes.toString(kv.getQualifier()).equals("birthdate")) {
                    person.setBirthDate(Bytes.toString(kv.getValue()));
                } else if (Bytes.toString(kv.getQualifier()).equals("city")) {
                    person.setCity(Bytes.toString(kv.getValue()));
                }
            } else if (Bytes.toString(kv.getFamily()).equals("friends")) {
                if (Bytes.toString(kv.getQualifier()).equals("bff")) {
                    person.setBff(Bytes.toString(kv.getValue()));
                } else if (Bytes.toString(kv.getQualifier()).equals("others")) {
                    person.setOtherFriends(Arrays.asList(Bytes.toString(kv.getValue()).split(",")));
                }
            }
        }
        return person;
    }

    public boolean personExists(String firstname) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(firstname.getBytes());
        Result result = table.get(get);
        if (result.size() == 0){
            return false;
        } else {return true;}
    }

}
