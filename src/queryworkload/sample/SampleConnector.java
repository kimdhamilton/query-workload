package queryworkload.sample;

import java.util.Map;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import queryworkload.ApiConnector;
import queryworkload.ApiException;
import queryworkload.Request;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class SampleConnector extends ApiConnector {


    private static final String API_HOST_PROPERTY = "apihost";
    private static final String API_PORT_PROPERTY = "apiport";

    private String apiHost;
    private String apiPort;
    
    // this is very expensive; reuse it
    private static Client client = Client.create();

    public SampleConnector(Properties props) throws ApiException {
        init(props);
    }

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init(Properties p) throws ApiException {
        apiHost = p.getProperty(API_HOST_PROPERTY);
        apiPort = p.getProperty(API_PORT_PROPERTY);
    }

    @Override
    public int call(Request request, Map<String, String> result) {

        String url = request.getUrl(apiHost, apiPort);
        int ret = 0;

        WebResource webResource = client.resource(url);

        ClientResponse response1 = webResource.accept("application", "json",
                "text").get(ClientResponse.class);

        if (response1.getStatus() != 200) {
            System.out.println("Failing while : " + url);
            throw new RuntimeException("Failed : HTTP error code : "
                    + response1.getStatus());
        }

        String output = response1.getEntity(String.class);

        try {
            JSONObject json = new JSONObject(output);
            JSONArray rowArray = json.getJSONArray("Row");
            ret = rowArray.length();
        } catch (JSONException e) {
            ret = -1;
            e.printStackTrace();
        }
        return ret;
    }

}
