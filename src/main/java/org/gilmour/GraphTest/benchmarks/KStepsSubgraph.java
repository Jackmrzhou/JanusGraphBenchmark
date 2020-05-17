package org.gilmour.GraphTest.benchmarks;

import javafx.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class KStepsSubgraph {
    private GraphTraversalSource g;
    public KStepsSubgraph(GraphTraversalSource g){
        this.g = g;
    }

    public Pair<Long, Double> RunKStepsSubgraph(Object vertexId, int k, String label) {
        long start = System.nanoTime();
        GraphTraversal<Vertex, Vertex> v = g.V(vertexId);
        Graph graph = (Graph) v.repeat(__.outE(label).subgraph("subGraph").inV()).times(k).cap("subGraph").next();
        long total = graph.traversal().V().count().next();
        long end = System.nanoTime();
        double ms = (end - start)*1.0 / 1000000;
        return new Pair<>(total, ms);
    }
}
