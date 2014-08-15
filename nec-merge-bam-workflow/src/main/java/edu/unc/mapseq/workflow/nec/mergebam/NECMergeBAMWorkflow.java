package edu.unc.mapseq.workflow.nec.mergebam;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.core.RemoveCLI;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk2.GATKUnifiedGenotyperCLI;
import edu.unc.mapseq.module.ic.CalculateMaximumLikelihoodFromVCFCLI;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.module.picard.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.picard.PicardMarkDuplicatesCLI;
import edu.unc.mapseq.module.picard.PicardMergeSAMCLI;
import edu.unc.mapseq.module.picard.PicardSortOrderType;
import edu.unc.mapseq.module.samtools.SAMToolsFlagstatCLI;
import edu.unc.mapseq.module.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowUtil;
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class NECMergeBAMWorkflow extends AbstractSampleWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NECMergeBAMWorkflow.class);

    public NECMergeBAMWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NECMergeBAMWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/nec/mergebam/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String saHome = getWorkflowBeanService().getAttributes().get("sequenceAnalysisHome");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");
        String unifiedGenotyperIntervalList = getWorkflowBeanService().getAttributes().get(
                "unifiedGenotyperIntervalList");
        String unifiedGenotyperDBSNP = getWorkflowBeanService().getAttributes().get("unifiedGenotyperDBSNP");
        String GATKKey = getWorkflowBeanService().getAttributes().get("GATKKey");
        String idCheckIntervalList = getWorkflowBeanService().getAttributes().get("idCheckIntervalList");
        String idCheckExomeChipData = getWorkflowBeanService().getAttributes().get("idCheckExomeChipData");

        File projectDirectory = new File(saHome, "NIDA");
        File cohortDirectory = new File(projectDirectory, "UCSF");
        File subjectParentDirectory = new File(cohortDirectory, "subjects");

        try {

            Workflow alignmentWorkflow = null;
            try {
                alignmentWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO()
                        .findByName("NECAlignment").get(0);
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
            }

            Set<String> subjectNameSet = new HashSet<String>();

            WorkflowRunAttempt attempt = getWorkflowRunAttempt();
            WorkflowRun workflowRun = attempt.getWorkflowRun();

            for (Sample sample : sampleSet) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                logger.info(sample.toString());

                Set<Attribute> attributeSet = workflowRun.getAttributes();
                if (attributeSet != null && !attributeSet.isEmpty()) {
                    Iterator<Attribute> attributeIter = attributeSet.iterator();
                    while (attributeIter.hasNext()) {
                        Attribute attribute = attributeIter.next();
                        String name = attribute.getName();
                        String value = attribute.getValue();
                        if ("subjectName".equals(name)) {
                            subjectNameSet.add(value);
                            break;
                        }
                    }
                }
            }

            Set<String> synchronizedSubjectNameSet = Collections.synchronizedSet(subjectNameSet);

            if (synchronizedSubjectNameSet.isEmpty()) {
                throw new WorkflowException("subjectNameSet is empty");
            }

            if (synchronizedSubjectNameSet.size() > 1) {
                throw new WorkflowException("multiple subjectName values across samples");
            }

            String subjectName = synchronizedSubjectNameSet.iterator().next();

            if (StringUtils.isEmpty(subjectName)) {
                throw new WorkflowException("empty subjectName");
            }

            // project directories
            File subjectDirectory = new File(subjectParentDirectory, subjectName);
            logger.info("subjectDirectory.getAbsolutePath(): {}", subjectDirectory.getAbsolutePath());

            File subjectFinalOutputDir = new File(subjectDirectory, "final");
            subjectFinalOutputDir.mkdirs();

            List<File> bamFileList = new ArrayList<File>();

            for (Sample sample : sampleSet) {

                if ("Undetermined".equals(sample.getBarcode())) {
                    continue;
                }

                File outputDirectory = new File(sample.getOutputDirectory());

                File bamFile = null;

                Set<FileData> fileDataSet = sample.getFileDatas();

                // 1st attempt to find bam file
                List<File> possibleVCFFileList = WorkflowUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                        getWorkflowBeanService().getMaPSeqDAOBean(), PicardAddOrReplaceReadGroups.class,
                        MimeType.APPLICATION_BAM, alignmentWorkflow.getId());

                if (possibleVCFFileList != null && possibleVCFFileList.size() > 0) {
                    bamFile = possibleVCFFileList.get(0);
                }

                // 2nd attempt to find bam file
                if (bamFile == null) {
                    for (File f : outputDirectory.listFiles()) {
                        if (f.getName().endsWith(".fixed-rg.bam")) {
                            bamFile = f;
                            break;
                        }
                    }
                }

                if (bamFile == null) {
                    throw new WorkflowException("No BAM file found");
                }

                bamFileList.add(bamFile);

            }

            // new job
            CondorJobBuilder builder = WorkflowJobFactory.createJob(++count, PicardMergeSAMCLI.class, attempt)
                    .siteName(siteName);
            File mergeBAMFilesOut = new File(subjectFinalOutputDir, String.format("%s.merged.bam", subjectName));
            builder.addArgument(PicardMergeSAMCLI.SORTORDER, "unsorted").addArgument(PicardMergeSAMCLI.OUTPUT,
                    mergeBAMFilesOut.getAbsolutePath());
            for (File f : bamFileList) {
                logger.info("Using file: {}", f.getAbsolutePath());
                builder.addArgument(PicardMergeSAMCLI.INPUT, f.getAbsolutePath());
            }
            CondorJob mergeBAMFilesJob = builder.build();
            logger.info(mergeBAMFilesJob.toString());
            graph.addVertex(mergeBAMFilesJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, PicardAddOrReplaceReadGroupsCLI.class, attempt).siteName(
                    siteName);
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
                    .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT,
                            picardAddOrReplaceReadGroupsOut.getAbsolutePath());
            CondorJob picardAddOrReplaceReadGroupsJob = builder.build();
            logger.info(picardAddOrReplaceReadGroupsJob.toString());
            graph.addVertex(picardAddOrReplaceReadGroupsJob);
            graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, PicardMarkDuplicatesCLI.class, attempt).siteName(siteName);
            File picardMarkDuplicatesOutput = new File(subjectFinalOutputDir, picardAddOrReplaceReadGroupsOut.getName()
                    .replace(".bam", ".deduped.bam"));
            File picardMarkDuplicatesMetrics = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName()
                    .replace(".bam", ".metrics"));
            builder.addArgument(PicardMarkDuplicatesCLI.INPUT, picardAddOrReplaceReadGroupsOut.getAbsolutePath())
                    .addArgument(PicardMarkDuplicatesCLI.OUTPUT, picardMarkDuplicatesOutput.getAbsolutePath())
                    .addArgument(PicardMarkDuplicatesCLI.METRICSFILE, picardMarkDuplicatesMetrics.getAbsolutePath());
            CondorJob picardMarkDuplicatesJob = builder.build();
            logger.info(picardMarkDuplicatesJob.toString());
            graph.addVertex(picardMarkDuplicatesJob);
            graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class, attempt).siteName(siteName);
            File samtoolsIndexOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                    ".bam", ".bai"));
            builder.addArgument(SAMToolsIndexCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath()).addArgument(
                    SAMToolsIndexCLI.OUTPUT, samtoolsIndexOutput.getAbsolutePath());
            CondorJob samtoolsIndexJob = builder.build();
            logger.info(samtoolsIndexJob.toString());
            graph.addVertex(samtoolsIndexJob);
            graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, SAMToolsFlagstatCLI.class, attempt).siteName(siteName);
            File samtoolsFlagstatOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                    ".bam", ".flagstat"));
            builder.addArgument(SAMToolsFlagstatCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath()).addArgument(
                    SAMToolsFlagstatCLI.OUTPUT, samtoolsFlagstatOutput.getAbsolutePath());
            CondorJob samtoolsFlagstatJob = builder.build();
            logger.info(samtoolsFlagstatJob.toString());
            graph.addVertex(samtoolsFlagstatJob);
            graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

            builder = WorkflowJobFactory.createJob(++count, GATKUnifiedGenotyperCLI.class, attempt).siteName(siteName)
                    .numberOfProcessors(4);
            File unifiedGenotyperOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                    ".bam", ".vcf"));
            File unifiedGenotyperMetrics = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName()
                    .replace(".bam", ".metrics"));
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
            logger.info(unifiedGenotyperJob.toString());
            graph.addVertex(unifiedGenotyperJob);
            graph.addEdge(samtoolsIndexJob, unifiedGenotyperJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, CalculateMaximumLikelihoodFromVCFCLI.class, attempt)
                    .siteName(siteName);
            builder.addArgument(CalculateMaximumLikelihoodFromVCFCLI.VCF, unifiedGenotyperOutput.getAbsolutePath())
                    .addArgument(CalculateMaximumLikelihoodFromVCFCLI.INTERVALLIST, idCheckIntervalList)
                    .addArgument(CalculateMaximumLikelihoodFromVCFCLI.SAMPLE,
                            unifiedGenotyperOutput.getName().replace(".vcf", ""))
                    .addArgument(CalculateMaximumLikelihoodFromVCFCLI.ECDATA, idCheckExomeChipData)
                    .addArgument(CalculateMaximumLikelihoodFromVCFCLI.OUTPUT, subjectFinalOutputDir.getAbsolutePath());
            CondorJob calculateMaximumLikelihoodsFromVCFJob = builder.build();
            logger.info(calculateMaximumLikelihoodsFromVCFJob.toString());
            graph.addVertex(calculateMaximumLikelihoodsFromVCFJob);
            graph.addEdge(unifiedGenotyperJob, calculateMaximumLikelihoodsFromVCFJob);

            // new job
            builder = WorkflowJobFactory.createJob(++count, RemoveCLI.class, attempt).siteName(siteName);
            builder.addArgument(RemoveCLI.FILE, mergeBAMFilesOut.getAbsolutePath()).addArgument(RemoveCLI.FILE,
                    picardAddOrReplaceReadGroupsOut.getAbsolutePath());
            CondorJob removeJob = builder.build();
            logger.info(removeJob.toString());
            graph.addVertex(removeJob);
            graph.addEdge(calculateMaximumLikelihoodsFromVCFJob, removeJob);

        } catch (Exception e) {
            throw new WorkflowException(e);
        }

        return graph;
    }
}
