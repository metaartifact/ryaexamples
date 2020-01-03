package query;

/***
 * This example queries data using RDF4J API method. Originally taken from Rya guideline and modified for compatibility with the environment
 * Author: Hammad Aslam Khan
 */

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import java.util.List;

public class Rdf4JAPIDataQuery {

    public static void main(String[] args) throws AccumuloSecurityException, AccumuloException {
        Connector connector = new ZooKeeperInstance("accumulo", "localhost:2181").getConnector("user", "password");

        final RdfCloudTripleStore store = new RdfCloudTripleStore();
        AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
        crdfdao.setConnector(connector);

        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("rts_");
        conf.setDisplayQueryPlan(true);
        crdfdao.setConf(conf);
        store.setRyaDAO(crdfdao);

        ProspectorServiceEvalStatsDAO evalDao = new ProspectorServiceEvalStatsDAO(connector, conf);
        evalDao.init();
        store.setRdfEvalStatsDAO(evalDao);

        InferenceEngine inferenceEngine = new InferenceEngine();
        inferenceEngine.setRyaDAO(crdfdao);
        inferenceEngine.setConf(conf);
        store.setInferenceEngine(inferenceEngine);

        Repository myRepository = new RyaSailRepository(store);
        myRepository.initialize();

        String query = "select * where {\n" +
                "<http://mynamespace/ProductType1> ?p ?o.\n" +
                "}";
        RepositoryConnection conn = myRepository.getConnection();
        System.out.println(query);
        TupleQuery tupleQuery = conn.prepareTupleQuery(
                QueryLanguage.SPARQL, query);
        ValueFactory vf = SimpleValueFactory.getInstance();

        TupleQueryResultHandler writer = new SPARQLResultsXMLWriter(System.out);
        tupleQuery.evaluate(new TupleQueryResultHandler() {

            int count = 0;

            @Override
            public void handleBoolean(boolean b) throws QueryResultHandlerException {

            }

            @Override
            public void handleLinks(List<String> list) throws QueryResultHandlerException {

            }

            @Override
            public void startQueryResult(List<String> strings) throws TupleQueryResultHandlerException {
            }

            @Override
            public void endQueryResult() throws TupleQueryResultHandlerException {
            }

            @Override
            public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {

                System.out.println(bindingSet);
            }
        });

        conn.close();
        myRepository.shutDown();
    }
}
