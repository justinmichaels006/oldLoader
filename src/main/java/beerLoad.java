import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.net.URI;
import java.util.ArrayList;

public class beerLoad {

    public static void main(String[] args) throws Exception {

    ArrayList<URI> nodes = new ArrayList<URI>();

    // Add one or more nodes of your cluster (exchange the IP with yours)
    nodes.add(URI.create("http://192.168.61.101:8091/pools"));
    nodes.add(URI.create("localhost"));

    // Try to connect to the client
    CouchbaseClient client = null;
    try {
        client = new CouchbaseClient(nodes, "beer-sample", "");
    } catch (Exception e) {
        System.err.println("Error connecting to Couchbase: " + e.getMessage());
        System.exit(1);
    }

    // Set a document
    client.set("hello", "couchbase!").get();
    // Return the result and validate
    String result = (String) client.get("hello");
    System.out.println(result);

    DescriptiveStatistics stats = new DescriptiveStatistics();

    View view = client.getView("beer", "allkeys");
        Query query = new Query();
        ViewResponse response = client.query(view, query);

        for (ViewRow row : response) {
            String theID = row.getId();
            long time1 = System.currentTimeMillis() % 1000;
            client.get(theID);
            long time2 = System.currentTimeMillis() % 1000;
            stats.addValue(time2 - time1);
        }

        // Compute some statistics
        double mean = stats.getMean();
        System.out.println("Mean:" + mean);
        double std = stats.getStandardDeviation();
        System.out.println("STD:" + std);
        double median = stats.getPercentile(95);
        System.out.println("MED:" + median);

    }

}
