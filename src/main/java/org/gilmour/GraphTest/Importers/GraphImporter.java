package org.gilmour.GraphTest.Importers;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.database.management.ManagementSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class GraphImporter {
    public static JanusGraph JanusG;
    public static int commitBatch = 4000;
    public static GraphTraversalSource g;
    public static String nonsensePayload;

    public static void doImport(String datasetPath, String confPath) throws Exception{
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1024; i++) {
            builder.append('c');
        }
        // 1kb payload
        nonsensePayload = builder.toString();
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
        config.addProperty("storage.backend", "hbase");
        config.addProperty("storage.hostname", "127.0.0.1");
        config.addProperty("cache.db-cache", false);
        JanusG = JanusGraphFactory.open(config);

        ManagementSystem mgmt = (ManagementSystem) JanusG.openManagement();
        mgmt.makeEdgeLabel("MyEdge").make();
        mgmt.makeVertexLabel("MyNode").make();
        PropertyKey id_key = mgmt.makePropertyKey("id").dataType(String.class).make();
        //properties for pageRank

        PropertyKey pageRank_key = mgmt.makePropertyKey("gremlin.pageRankVertexProgram.pageRank").dataType(Double.class).make();
        PropertyKey edgeCount_key = mgmt.makePropertyKey("gremlin.pageRankVertexProgram.edgeCount").dataType(Long.class).make();

        //properties for WccBenchmark

        PropertyKey groupId_key = mgmt.makePropertyKey("WCC.groupId").dataType(Long.class).make();


        mgmt.buildIndex("byId", JanusGraphVertex.class).addKey(id_key).unique().buildCompositeIndex();
        mgmt.commit();
        boolean indexDone = false;
        while (! indexDone)
            try{
                mgmt.awaitGraphIndexStatus(JanusG, "byId").call();
                indexDone = true;
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }

        g = JanusG.traversal();

        try {

            BufferedReader reader = new BufferedReader(new InputStreamReader(GraphImporter.class.getClassLoader().getResourceAsStream(datasetPath)));
            String line;
            long lineCounter = 0;
            long startTime = System.nanoTime();
            while((line = reader.readLine()) != null) {
                try {
                    if (line.startsWith("#")){
                        continue;
                    }
                    String[] parts = line.split("\t");

                    processLine(parts[0], parts[1]);

                    lineCounter++;
                    if(lineCounter % commitBatch == 0){
                        System.out.println("---- commit ----: " + Long.toString(lineCounter / commitBatch));
                        g.tx().commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            g.tx().commit();
            JanusG.tx().commit();
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);
            System.out.println("######## loading time #######  " + Long.toString(duration/1000000) + " ms");
            reader.close();
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
        System.out.println("---- done ----, total V: " + Long.toString(g.V().count().next()));
        System.exit(0);
    }

    private static Vertex getOrCreate(String id) {
       return   g.V().has("MyNode","id", id).fold().coalesce(__.unfold(),
                        __.addV("MyNode").property("id", id).
                                property("gremlin.pageRankVertexProgram.pageRank", 1.0).
                                property("gremlin.pageRankVertexProgram.edgeCount", 0).
                                property("WCC.groupId", Long.parseLong(id)).
                                property("nonsenseKey", nonsensePayload)).next();
    }

    /** This function add vertex and edge
     * @param srcId the source vertex of the edge
     * @param dstId the destination vertex of the edge
     */

    private static void processLine(String srcId, String dstId) {
        Vertex from, to;
        from = getOrCreate(srcId);
        to = getOrCreate(dstId);
        g.addE("MyEdge").from(from).to(to).iterate();
    }
}
