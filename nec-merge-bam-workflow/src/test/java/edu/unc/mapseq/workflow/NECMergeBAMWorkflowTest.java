package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorDOTExporter;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.CondorJobVertexNameProvider;

import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.gatk2.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.ic.CalculateMaximumLikelihoodFromVCFCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.picard.PicardMergeSAMCLI;
import edu.unc.mapseq.module.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;

public class NECMergeBAMWorkflowTest {

    public NECMergeBAMWorkflowTest() throws WorkflowException {
        super();
    }

    @Test
    public void createGraph() throws WorkflowException {

        int count = 0;

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        // new job
        CondorJob mergeBAMFilesJob = new CondorJob(String.format("%s_%d", PicardMergeSAMCLI.class.getSimpleName(),
                ++count), null);
        graph.addVertex(mergeBAMFilesJob);

        // new job
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJob(String.format("%s_%d",
                PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

        // new job
        CondorJob picardMarkDuplicatesJob = new CondorJob(String.format("%s_%d",
                PicardMarkDuplicatesCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

        // new job
        CondorJob samtoolsIndexJob = new CondorJob(String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(),
                ++count), null);
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        CondorJob samtoolsFlagstatJob = new CondorJob(String.format("%s_%d", SAMToolsFlagstatCLI.class.getSimpleName(),
                ++count), null);
        graph.addVertex(samtoolsFlagstatJob);
        graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

        // new job
        CondorJob unifiedGenotyperJob = new CondorJob(String.format("%s_%d",
                GATKUnifiedGenotyperCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(unifiedGenotyperJob);
        graph.addEdge(samtoolsIndexJob, unifiedGenotyperJob);

        // new job
        CondorJob calculateMaximumLikelihoodsFromVCFJob = new CondorJob(String.format("%s_%d",
                CalculateMaximumLikelihoodFromVCFCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(calculateMaximumLikelihoodsFromVCFJob);
        graph.addEdge(unifiedGenotyperJob, calculateMaximumLikelihoodsFromVCFJob);

        // new job
        CondorJob removeJob = new CondorJob(String.format("%s_%d", RemoveCLI.class.getSimpleName(), ++count), null);
        graph.addVertex(removeJob);
        graph.addEdge(calculateMaximumLikelihoodsFromVCFJob, removeJob);

        CondorJobVertexNameProvider vnp = new CondorJobVertexNameProvider();
        CondorDOTExporter<CondorJob, CondorJobEdge> dotExporter = new CondorDOTExporter<CondorJob, CondorJobEdge>(vnp,
                vnp, null, null, null, null);
        File srcSiteResourcesImagesDir = new File("src/site/resources/images");
        if (!srcSiteResourcesImagesDir.exists()) {
            srcSiteResourcesImagesDir.mkdirs();
        }
        File dotFile = new File(srcSiteResourcesImagesDir, "workflow.dag.dot");
        try {
            FileWriter fw = new FileWriter(dotFile);
            dotExporter.export(fw, graph);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
