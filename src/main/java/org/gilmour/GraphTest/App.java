package org.gilmour.GraphTest;

import javafx.util.Pair;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.gilmour.GraphTest.Importers.GraphImporter;
import org.gilmour.GraphTest.benchmarks.KStepsNeighbor;
import org.gilmour.GraphTest.benchmarks.PageRank;
import org.gilmour.GraphTest.benchmarks.WccBenchmark;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.List;

/**
 * Hello world!
 *
 */
public class App
{

    public static void ImportGraph() throws Exception{
        String dataPath = "data/web-NotreDame.txt";
        String confPath = "conf/janusgraph-hbase.properties";
        GraphImporter.doImport(dataPath, confPath);
    }

    public static void main( String[] args )
    {
//        Graph graph = EmptyGraph.instance();
//        GraphTraversalSource g = graph.traversal().withRemote("conf/remote-graph.properties");
        // Reuse 'g' across the application
        // and close it on shut-down to close open connections with g.close()
//        int sampleTotal = 10;
//        List<Vertex> vertices = g.V().sample(sampleTotal).next(10);
//        KStepsNeighbor kStepsNeighbor =new KStepsNeighbor(g);
//        for (Vertex v : vertices) {
//            Pair<Long, Double> res = kStepsNeighbor.RunKStepsNeighbor(v.id(), 2, "votesFor");
//            System.out.printf("K steps neighbor, K = 2; Start Vertex: %s, userId : %s; Total vertices found: %d; Time cost: %.3fms;\n",
//                    v.id(), g.V(v.id()).values("userId").next(), res.getKey(), res.getValue());
//        }
//        KStepsSubgraph subgraph = new KStepsSubgraph(g);
//        for (Vertex v : vertices) {
//            Pair<Long, Double> res = subgraph.RunKStepsSubgraph(v.id(), 2, "votesFor");
//            System.out.printf("Subgraph, k = 2, total vertices got: %d; time cost: %.2fms\n", res.getKey(), res.getValue());
//        }

        //ImportGraph();


        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
        config.addProperty("storage.backend", "hbase");
        config.addProperty("storage.hostname", "127.0.0.1");
        config.addProperty("cache.db-cache", false);
        JanusGraph janusGraph = JanusGraphFactory.open(config);
        PageRank pageRank = new PageRank(janusGraph);
        try {
            pageRank.runPageRank();
        } catch (Exception e){
            e.printStackTrace();
        }

        WccBenchmark wccBenchmark = new WccBenchmark(janusGraph);
        try {
            wccBenchmark.runWcc();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
