package org.gilmour.GraphTest.benchmarks;

import javafx.util.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class KStepsNeighbor {
    private GraphTraversalSource g;
    public KStepsNeighbor(GraphTraversalSource g) {
        this.g = g;
    }

    public Pair<Long, Double> RunKStepsNeighbor(Object vertexId, int k, String label) {
        long start = System.nanoTime();
        GraphTraversal<Vertex, Vertex> v = g.V(vertexId);
        for (int i = 0; i < k; i++){
            v = v.out(label);
        }
        long total = v.dedup().count().next();
        long end = System.nanoTime();
        double ms = (end - start)*1.0 / 1000000;
        return new Pair<>(total, ms);
    }
}
