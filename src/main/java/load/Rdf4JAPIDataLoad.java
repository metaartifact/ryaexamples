package load;

/***
 * This example loads data on RDF using RDF4J method. Originally taken from Rya guideline and modified for compatibility with the environment
 * Author: Hammad Aslam Khan
 */

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class Rdf4JAPIDataLoad {

    public static void main(String[] args) throws IOException, AccumuloSecurityException, AccumuloException {
        final RdfCloudTripleStore store = new RdfCloudTripleStore();
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        AccumuloRyaDAO dao = new AccumuloRyaDAO();
        Connector connector = new ZooKeeperInstance("accumulo", "localhost:2181").getConnector("root", "root");
        dao.setConnector(connector);
        conf.setTablePrefix("rya_");
        dao.setConf(conf);
        store.setRyaDAO(dao);

        Repository myRepository = new RyaSailRepository(store);
        myRepository.initialize();
        RepositoryConnection conn = myRepository.getConnection();


        //load data from file
        final File file = new File(Thread.currentThread().getContextClassLoader().getResource("$RDF_DATA").getFile());
        conn.add(new FileInputStream(file), file.getName(),
                RDFFormat.NTRIPLES, new Resource[]{});

        conn.commit();

        conn.close();
        myRepository.shutDown();
    }
}
