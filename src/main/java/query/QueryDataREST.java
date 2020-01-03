package query;

/***
 * This example queries data using REST API method. Originally taken from Rya guideline and modified for compatibility with the environment
 * Author: Hammad Aslam Khan
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

public class QueryDataREST {

    public static void main(String[] args) {
        try {
            String query = "select * where {\n" +
                    "<http://mynamespace/ProductType1> ?p ?o.\n" +
                    "}";

            String queryenc = URLEncoder.encode(query, "UTF-8");

            URL url = new URL("http://localhost:8080/web.rya/queryrdf?query=" + queryenc);
            URLConnection urlConnection = url.openConnection();
            urlConnection.setDoOutput(true);

            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
