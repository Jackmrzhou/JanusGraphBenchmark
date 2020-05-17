package org.gilmour.GraphTest.benchmarks;

import org.gilmour.GraphTest.benchmarks.Components.PG;
import org.gilmour.GraphTest.benchmarks.Components.WCC;
import org.janusgraph.core.JanusGraph;

import java.util.concurrent.Future;

public class WccBenchmark {
    private JanusGraph JanusG;
    public WccBenchmark(JanusGraph janusG){
        JanusG = janusG;
    }

    public void runWcc() throws  Exception{
        long startTime = System.nanoTime();
        Future result = JanusG.compute().workers(4).program(WCC.build().create(JanusG)).submit();
        result.get();
        long duration = System.nanoTime() - startTime;
        System.out.printf("Wcc time cost: %.3fms\n", (double)duration / 1000000);
    }
}
