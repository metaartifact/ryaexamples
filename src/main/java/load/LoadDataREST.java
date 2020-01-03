package load;

/***
 * This example loads data on RDF using POST method. Originally taken from Rya guideline and modified for compatibility with the environment
 * Author: Hammad Aslam Khan
 */

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

public class LoadDataREST {
    public static void main(String[] args){
        try {
            final InputStream resourceAsStream = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("$RDF_DATA");
            URL url = new URL("http://localhost:8080/web.rya/loadrdf" +
                    "?format=N-Triples" +
                    "");
            URLConnection urlConnection = url.openConnection();
            urlConnection.setRequestProperty("Content-Type", "text/plain");
            urlConnection.setDoOutput(true);

            final OutputStream os = urlConnection.getOutputStream();

            int read;
            while((read = resourceAsStream.read()) >= 0) {
                os.write(read);
            }
            resourceAsStream.close();
            os.flush();

            BufferedReader rd = new BufferedReader(new InputStreamReader(
                    urlConnection.getInputStream()));
            String line;
            while ((line = rd.readLine()) != null) {
                System.out.println(line);
            }
            rd.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
