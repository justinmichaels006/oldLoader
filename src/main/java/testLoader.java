import com.couchbase.client.CouchbaseClient;
import com.oracle.javafx.jmx.json.JSONDocument;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class testLoader {

    public static void main(String[] args) throws Exception {

        ArrayList<URI> nodes = new ArrayList<URI>();

        // Add one or more nodes of your cluster (exchange the IP with yours)
        nodes.add(URI.create("http://192.168.61.101:8091/pools"));

        // Try to connect to the client
        CouchbaseClient client = null;
        try {
            client = new CouchbaseClient(nodes, "testload", "password");
        } catch (Exception e) {
            System.err.println("Error connecting to Couchbase: " + e.getMessage());
            System.exit(1);
        }

        List<JSONDocument> docArray2 = new ArrayList();

        String sCurrentLine;
        JSONParser parser = new JSONParser();
        String obj;

        BufferedReader br1;
        br1 = new BufferedReader(new FileReader("/Users/justin/Documents/Demo/data/stocks.json"));
        final DescriptiveStatistics stats = new DescriptiveStatistics();
        final CountDownLatch latch = new CountDownLatch(6756);

            while ((sCurrentLine = br1.readLine()) != null) {
                try {
                    obj = parser.parse(sCurrentLine).toString();
                    String theid = UUID.randomUUID().toString();
                    //DEBUG: System.out.println(theid);
                    final long time1 = System.currentTimeMillis();

                    client.set(theid, obj).addListener(new OperationCompletionListener() {
                        public void onComplete(OperationFuture<?> future) throws Exception {
                            long time2 = System.currentTimeMillis();
                            stats.addValue(time2 - time1);
                            latch.countDown();
                        }
                    });
                } catch (ParseException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        boolean success = latch.await(10, TimeUnit.MINUTES);
        if (success) {
            System.out.println("I'm done!");
            // Compute some statistics
            double mean = stats.getMean();
            System.out.println("Mean Set:" + mean);
            double std = stats.getStandardDeviation();
            System.out.println("STD Set:" + std);
            double median = stats.getPercentile(95);
            System.out.println("MED Set:" + median);
        } else {
            System.out.println("ouch");
        }

        client.shutdown();

    }
}
