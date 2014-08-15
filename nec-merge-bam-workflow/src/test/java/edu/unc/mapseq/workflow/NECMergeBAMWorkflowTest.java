package edu.unc.mapseq.workflow;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.renci.jlrm.condor.ext.CondorDOTExporter;
import org.renci.jlrm.condor.ext.CondorJobVertexNameProvider;

import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk2.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.ic.CalculateMaximumLikelihoodFromVCFCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.picard.PicardMergeSAMCLI;
import edu.unc.mapseq.module.picard.PicardSortOrderType;
import edu.unc.mapseq.module.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;
import edu.unc.mapseq.workflow.impl.exporter.CLIScriptExporter;

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
        CondorJob mergeBAMFilesJob = new CondorJobBuilder().name(
                String.format("%s_%d", PicardMergeSAMCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(mergeBAMFilesJob);

        // new job
        CondorJob picardAddOrReplaceReadGroupsJob = new CondorJobBuilder().name(
                String.format("%s_%d", PicardAddOrReplaceReadGroupsCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

        // new job
        CondorJob picardMarkDuplicatesJob = new CondorJobBuilder().name(
                String.format("%s_%d", PicardMarkDuplicatesCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

        // new job
        CondorJob samtoolsIndexJob = new CondorJobBuilder().name(
                String.format("%s_%d", SAMToolsIndexCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        CondorJob samtoolsFlagstatJob = new CondorJobBuilder().name(
                String.format("%s_%d", SAMToolsFlagstatCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(samtoolsFlagstatJob);
        graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

        // new job
        CondorJob unifiedGenotyperJob = new CondorJobBuilder().name(
                String.format("%s_%d", GATKUnifiedGenotyperCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(unifiedGenotyperJob);
        graph.addEdge(samtoolsIndexJob, unifiedGenotyperJob);

        // new job
        CondorJob calculateMaximumLikelihoodsFromVCFJob = new CondorJobBuilder().name(
                String.format("%s_%d", CalculateMaximumLikelihoodFromVCFCLI.class.getSimpleName(), ++count)).build();
        graph.addVertex(calculateMaximumLikelihoodsFromVCFJob);
        graph.addEdge(unifiedGenotyperJob, calculateMaximumLikelihoodsFromVCFJob);

        // new job
        CondorJob removeJob = new CondorJobBuilder().name(
                String.format("%s_%d", RemoveCLI.class.getSimpleName(), ++count)).build();
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

    @Test
    public void showCommands() {

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);
        String subjectName = "qwerasdfzcxv";

        String siteName = "Kure";
        int count = 0;

        String idCheckIntervalList = "/proj/renci/nec/identity-check/resources/ucsf_ec_snp.interval_list";
        String idCheckExomeChipData = "/proj/renci/nec/identity-check/resources/ucsf_ec_data.tsv";
        String referenceSequence = "/proj/renci/sequence_analysis/references/BUILD.37.1/bwa061sam0118/BUILD.37.1.sorted.shortid.fa";
        String GATKKey = "/proj/renci/sequence_analysis/resources/gatk/key/xiao_renci.org.key";
        String unifiedGenotyperIntervalList = "/proj/renci/rc_renci/resources/nida/ec.shortid.interval_list";
        String unifiedGenotyperDBSNP = "/proj/renci/sequence_analysis/resources/gatk/bundle/1.5/b37/dbsnp_135.b37.renci.sorted.shortid.vcf";

        File subjectFinalOutputDir = new File("/tmp", "subjects");

        List<File> bamFileList = Arrays.asList(new File("/tmp", "qwer.bam"), new File("/tmp", "asdf.bam"), new File(
                "/tmp", "zxcv.bam"));

        // new job
        CondorJobBuilder builder = WorkflowJobFactory.createDryRunJob(++count, PicardMergeSAMCLI.class).siteName(
                siteName);
        File mergeBAMFilesOut = new File(subjectFinalOutputDir, String.format("%s.merged.bam", subjectName));
        builder.addArgument(PicardMergeSAMCLI.SORTORDER, "unsorted").addArgument(PicardMergeSAMCLI.OUTPUT,
                mergeBAMFilesOut.getAbsolutePath());
        for (File f : bamFileList) {
            builder.addArgument(PicardMergeSAMCLI.INPUT, f.getAbsolutePath());
        }
        CondorJob mergeBAMFilesJob = builder.build();
        graph.addVertex(mergeBAMFilesJob);

        // new job
        builder = WorkflowJobFactory.createDryRunJob(++count, PicardAddOrReplaceReadGroupsCLI.class).siteName(siteName);
        File picardAddOrReplaceReadGroupsOut = new File(subjectFinalOutputDir, mergeBAMFilesOut.getName().replace(
                ".bam", ".rg.bam"));
        builder.addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT, mergeBAMFilesOut.getAbsolutePath())
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPID, subjectName)
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, subjectName)
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM, "Illumina HiSeq 2000")
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT, subjectName)
                .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME, subjectName)
                .addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER,
                        PicardSortOrderType.COORDINATE.toString().toLowerCase())
                .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath());
        CondorJob picardAddOrReplaceReadGroupsJob = builder.build();
        graph.addVertex(picardAddOrReplaceReadGroupsJob);
        graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

        // new job
        builder = WorkflowJobFactory.createDryRunJob(++count, PicardMarkDuplicatesCLI.class).siteName(siteName);
        File picardMarkDuplicatesOutput = new File(subjectFinalOutputDir, picardAddOrReplaceReadGroupsOut.getName()
                .replace(".bam", ".deduped.bam"));
        File picardMarkDuplicatesMetrics = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName()
                .replace(".bam", ".metrics"));
        builder.addArgument(PicardMarkDuplicatesCLI.INPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath())
                .addArgument(PicardMarkDuplicatesCLI.OUTPUT, picardMarkDuplicatesOutput.getAbsolutePath())
                .addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetrics.getAbsolutePath());
        CondorJob picardMarkDuplicatesJob = builder.build();
        graph.addVertex(picardMarkDuplicatesJob);
        graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

        // new job
        builder = WorkflowJobFactory.createDryRunJob(++count, SAMToolsIndexCLI.class).siteName(siteName);
        File samtoolsIndexOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(".bam",
                ".bai"));
        builder.addArgument(SAMToolsIndexCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath()).addArgument(
                SAMToolsIndexCLI.OUTPUT, samtoolsIndexOutput.getAbsolutePath());
        CondorJob samtoolsIndexJob = builder.build();
        graph.addVertex(samtoolsIndexJob);
        graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

        // new job
        builder = WorkflowJobFactory.createDryRunJob(++count, SAMToolsFlagstatCLI.class).siteName(siteName);
        File samtoolsFlagstatOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                ".bam", ".flagstat"));
        builder.addArgument(SAMToolsFlagstatCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath()).addArgument(
                SAMToolsFlagstatCLI.OUTPUT, samtoolsFlagstatOutput.getAbsolutePath());
        CondorJob samtoolsFlagstatJob = builder.build();
        graph.addVertex(samtoolsFlagstatJob);
        graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

        builder = WorkflowJobFactory.createDryRunJob(++count, GATKUnifiedGenotyperCLI.class).siteName(siteName)
                .numberOfProcessors(4);
        File unifiedGenotyperOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                ".bam", ".vcf"));
        File unifiedGenotyperMetrics = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                ".bam", ".metrics"));
        builder.addArgument(GATKUnifiedGenotyperCLI.INPUTFILE, picardMarkDuplicatesOutput.getAbsolutePath())
                .addArgument(GATKUnifiedGenotyperCLI.OUT, unifiedGenotyperOutput.getAbsolutePath())
                .addArgument(GATKUnifiedGenotyperCLI.KEY, GATKKey)
                .addArgument(GATKUnifiedGenotyperCLI.INTERVALS, unifiedGenotyperIntervalList)
                .addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence)
                .addArgument(GATKUnifiedGenotyperCLI.DBSNP, unifiedGenotyperDBSNP)
                .addArgument(GATKUnifiedGenotyperCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString())
                .addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString())
                .addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH")
                .addArgument(GATKUnifiedGenotyperCLI.OUTPUTMODE, "EMIT_ALL_SITES")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "AlleleBalance")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "DepthOfCoverage")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HomopolymerRun")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "MappingQualityZero")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "QualByDepth")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "RMSMappingQuality")
                .addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HaplotypeScore")
                .addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLETOCOVERAGE, "250")
                .addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "4")
                .addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0")
                .addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4")
                .addArgument(GATKUnifiedGenotyperCLI.METRICS, unifiedGenotyperMetrics.getAbsolutePath());
        CondorJob unifiedGenotyperJob = builder.build();
        graph.addVertex(unifiedGenotyperJob);
        graph.addEdge(samtoolsIndexJob, unifiedGenotyperJob);

        // new job
        builder = WorkflowJobFactory.createDryRunJob(++count, CalculateMaximumLikelihoodFromVCFCLI.class).siteName(
                siteName);
        builder.addArgument(CalculateMaximumLikelihoodFromVCFCLI.VCF, unifiedGenotyperOutput.getAbsolutePath())
                .addArgument(CalculateMaximumLikelihoodFromVCFCLI.INTERVALLIST, idCheckIntervalList)
                .addArgument(CalculateMaximumLikelihoodFromVCFCLI.SAMPLE,
                        unifiedGenotyperOutput.getName().replace(".vcf", ""))
                .addArgument(CalculateMaximumLikelihoodFromVCFCLI.ECDATA, idCheckExomeChipData)
                .addArgument(CalculateMaximumLikelihoodFromVCFCLI.OUTPUT, subjectFinalOutputDir.getAbsolutePath());
        CondorJob calculateMaximumLikelihoodsFromVCFJob = builder.build();
        graph.addVertex(calculateMaximumLikelihoodsFromVCFJob);
        graph.addEdge(unifiedGenotyperJob, calculateMaximumLikelihoodsFromVCFJob);

        // new job
        builder = WorkflowJobFactory.createDryRunJob(++count, RemoveCLI.class).siteName(siteName);
        builder.addArgument(RemoveCLI.FILE, mergeBAMFilesOut.getAbsolutePath()).addArgument(RemoveCLI.FILE,
                picardAddOrReplaceReadGroupsOut.getAbsolutePath());
        CondorJob removeJob = builder.build();
        graph.addVertex(removeJob);
        graph.addEdge(calculateMaximumLikelihoodsFromVCFJob, removeJob);

        File workDir = new File("/tmp/workflows");
        workDir.mkdirs();

        CLIScriptExporter exporter = new CLIScriptExporter();
        exporter.export("NECMergeBam", workDir, graph);
    }
}
