package org.gilmour.GraphTest.benchmarks;

import org.gilmour.GraphTest.benchmarks.Components.PG;
import org.janusgraph.core.JanusGraph;

import java.util.concurrent.Future;

public class PageRank {
    private JanusGraph JanusG;
    public PageRank(JanusGraph janusG){
        JanusG = janusG;
    }

    public void runPageRank() throws  Exception{
        long startTime = System.nanoTime();
        Future result = JanusG.compute().workers(4).program(PG.build().create(JanusG)).submit();
        result.get();
        long duration = System.nanoTime() - startTime;
        System.out.printf("page rank time cost: %.3fms\n", (double)duration / 1000000);
    }
}
