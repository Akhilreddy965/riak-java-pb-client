import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.*;
import com.basho.riak.client.api.commands.kv.StoreValue.Builder;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue.Builder;
import com.basho.riak.client.api.RiakResponse;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.SortOrder;

import java.util.concurrent.ExecutionException;

public class RiakExample {

    public static void main(String[] args) {
        // The Riak client needs to be connected to the Riak cluster.
        RiakClient client = RiakClient.newClient("127.0.0.1");  // Adjust to the Riak server's IP address

        try {
            // Define the bucket type and name
            Namespace namespace = new Namespace("default", "my_bucket");

            // Store a value
            String key = "my_key";
            String value = "Hello, Riak!";

            StoreValue storeOp = new Builder()
                    .withNamespace(namespace)
                    .withKey(key)
                    .withValue(value)
                    .build();

            RiakResponse storeResponse = client.execute(storeOp);
            System.out.println("Stored value: " + value);

            // Fetch the value
            FetchValue fetchOp = new FetchValue.Builder(namespace, key).build();
            FetchValue.Response fetchResponse = client.execute(fetchOp);

            if (fetchResponse.hasValue()) {
                System.out.println("Fetched value: " + fetchResponse.getValue().toString());
            } else {
                System.out.println("Key not found.");
            }

            // Delete the value
            DeleteValue deleteOp = new DeleteValue.Builder(namespace, key).build();
            client.execute(deleteOp);
            System.out.println("Deleted value with key: " + key);

        } catch (ExecutionException | InterruptedException e) {
            System.err.println("Error interacting with Riak: " + e.getMessage());
        } finally {
            // Close the client when done
            try {
                client.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
