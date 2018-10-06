import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.conf.Configuration;
import org.fluttercode.datafactory.impl.DataFactory;
import org.joda.time.DateTime;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import java.util.*;

public class GenerateData {
    public static void main(String[] args) throws IOException {

        // Instantiating configuration class
        Configuration con = HBaseConfiguration.create();

        String service_provider = args[0];
        String tablename = "cdr_test_" + service_provider;

        DataFactory df = new DataFactory();
        Random r = new Random();
        ArrayList<String> providers = new ArrayList<String>(Arrays.asList("MT","HT","ET","BA","DA"));

        HashMap<String, String> provider_numcode = new HashMap<String, String>();
        provider_numcode.put("MT", "071");
        provider_numcode.put("HT", "078");
        provider_numcode.put("ET", "072");
        provider_numcode.put("BA", "075");
        provider_numcode.put("DA", "077");

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("cdr_id"));
        tableDescriptor.addFamily(new HColumnDescriptor("calling_num"));
        tableDescriptor.addFamily(new HColumnDescriptor("calling_tower"));
        tableDescriptor.addFamily(new HColumnDescriptor("called_num"));
        tableDescriptor.addFamily(new HColumnDescriptor("called_tower"));
        tableDescriptor.addFamily(new HColumnDescriptor("date_time"));
        tableDescriptor.addFamily(new HColumnDescriptor("duration"));

        // Instantiating HTable class
        HTable hTable = new HTable(con, tablename);

        for (int i = 1; i <= Integer.parseInt(args[1]); i++) {

            // cdr ID
            UUID id = UUID.randomUUID();

            // calling party
            String calling_num = provider_numcode.get(service_provider) + df.getNumberText(7);
            String calling_tower_id = service_provider + r.nextInt(10000);

            // recipient
            String provider_code = providers.get(r.nextInt(5));
            String called_num = provider_numcode.get(provider_code) + df.getNumberText(7);
            String recipient_tower_id = provider_code + r.nextInt(10000);

            // date and time
            long t1 = System.currentTimeMillis() + r.nextInt();
            DateTime d1 = new DateTime(t1);

            // duration in seconds
            int duration = r.nextInt(200);

            // Instantiating Put class
            // accepts a row name.
            Put p = new Put(Bytes.toBytes(i));

            // adding values using add() method
            // accepts column family name, qualifier/row name ,value
            p.add(Bytes.toBytes("cdr_id"), Bytes.toBytes("cdr_id"), Bytes.toBytes(id.toString()));
            p.add(Bytes.toBytes("calling_num"), Bytes.toBytes("calling_num"), Bytes.toBytes(calling_num));
            p.add(Bytes.toBytes("calling_tower"), Bytes.toBytes("calling_tower"), Bytes.toBytes(calling_tower_id));
            p.add(Bytes.toBytes("called_num"), Bytes.toBytes("called_num"), Bytes.toBytes(called_num));
            p.add(Bytes.toBytes("called_tower"), Bytes.toBytes("called_tower"), Bytes.toBytes(recipient_tower_id));
            p.add(Bytes.toBytes("date_time"), Bytes.toBytes("date_time"), Bytes.toBytes(d1.toString()));
            p.add(Bytes.toBytes("duration"), Bytes.toBytes("duration"), Bytes.toBytes(String.valueOf(duration)));

            hTable.put(p);
            System.out.println("data inserted "+i);
        }
        // closing HTable
        hTable.close();
        System.out.println(" Table created ");
    }

}
