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
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.dao.model.Workflow;
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
import edu.unc.mapseq.workflow.AbstractWorkflow;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowJobFactory;
import edu.unc.mapseq.workflow.WorkflowUtil;

public class NECMergeBAMWorkflow extends AbstractWorkflow {

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

        Set<HTSFSample> htsfSampleSet = getAggregateHTSFSampleSet();
        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

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
                        .findByName("NECAlignment");
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
            }

            Set<String> subjectNameSet = new HashSet<String>();

            for (HTSFSample htsfSample : htsfSampleSet) {

                if ("Undetermined".equals(htsfSample.getBarcode())) {
                    continue;
                }

                logger.info("htsfSample: {}", htsfSample.toString());

                Set<EntityAttribute> attributeSet = htsfSample.getAttributes();
                Iterator<EntityAttribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    EntityAttribute attribute = attributeIter.next();
                    String name = attribute.getName();
                    String value = attribute.getValue();
                    if ("subjectName".equals(name)) {
                        subjectNameSet.add(value);
                        break;
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

            for (HTSFSample htsfSample : htsfSampleSet) {

                if ("Undetermined".equals(htsfSample.getBarcode())) {
                    continue;
                }

                SequencerRun sequencerRun = htsfSample.getSequencerRun();
                File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample,
                        getName().replace("MergeBAM", ""), getVersion());

                File bamFile = null;

                Set<FileData> fileDataSet = htsfSample.getFileDatas();

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
            CondorJob mergeBAMFilesJob = WorkflowJobFactory.createJob(++count, PicardMergeSAMCLI.class,
                    getWorkflowPlan());
            mergeBAMFilesJob.setSiteName(siteName);
            mergeBAMFilesJob.addArgument(PicardMergeSAMCLI.SORTORDER, "unsorted");
            File mergeBAMFilesOut = new File(subjectFinalOutputDir, String.format("%s.merged.bam", subjectName));
            mergeBAMFilesJob.addArgument(PicardMergeSAMCLI.OUTPUT, mergeBAMFilesOut.getAbsolutePath());
            for (File f : bamFileList) {
                logger.info("Using file: {}", f.getAbsolutePath());
                mergeBAMFilesJob.addArgument(PicardMergeSAMCLI.INPUT, f.getAbsolutePath());
            }
            graph.addVertex(mergeBAMFilesJob);

            // new job
            CondorJob picardAddOrReplaceReadGroupsJob = WorkflowJobFactory.createJob(++count,
                    PicardAddOrReplaceReadGroupsCLI.class, getWorkflowPlan());
            picardAddOrReplaceReadGroupsJob.setSiteName(siteName);
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT,
                    mergeBAMFilesOut.getAbsolutePath());
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC");
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPID, subjectName);
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, subjectName);
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM,
                    "Illumina HiSeq 2000");
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT,
                    subjectName);
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME,
                    subjectName);
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER,
                    PicardSortOrderType.COORDINATE.toString().toLowerCase());
            File picardAddOrReplaceReadGroupsOut = new File(subjectFinalOutputDir, mergeBAMFilesOut.getName().replace(
                    ".bam", ".rg.bam"));
            picardAddOrReplaceReadGroupsJob.addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT,
                    picardAddOrReplaceReadGroupsOut.getAbsolutePath());
            graph.addVertex(picardAddOrReplaceReadGroupsJob);
            graph.addEdge(mergeBAMFilesJob, picardAddOrReplaceReadGroupsJob);

            // new job
            CondorJob picardMarkDuplicatesJob = WorkflowJobFactory.createJob(++count, PicardMarkDuplicatesCLI.class,
                    getWorkflowPlan());
            picardMarkDuplicatesJob.setSiteName(siteName);
            picardMarkDuplicatesJob.addArgument(PicardMarkDuplicatesCLI.INPUT,
                    picardAddOrReplaceReadGroupsOut.getAbsolutePath());
            File picardMarkDuplicatesOutput = new File(subjectFinalOutputDir, picardAddOrReplaceReadGroupsOut.getName()
                    .replace(".bam", ".deduped.bam"));
            picardMarkDuplicatesJob.addArgument(PicardMarkDuplicatesCLI.OUTPUT,
                    picardMarkDuplicatesOutput.getAbsolutePath());
            File picardMarkDuplicatesMetrics = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName()
                    .replace(".bam", ".metrics"));
            picardMarkDuplicatesJob.addArgument(PicardMarkDuplicatesCLI.METRICSFILE,
                    picardMarkDuplicatesMetrics.getAbsolutePath());
            graph.addVertex(picardMarkDuplicatesJob);
            graph.addEdge(picardAddOrReplaceReadGroupsJob, picardMarkDuplicatesJob);

            // new job
            CondorJob samtoolsIndexJob = WorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class,
                    getWorkflowPlan());
            samtoolsIndexJob.setSiteName(siteName);
            samtoolsIndexJob.addArgument(SAMToolsIndexCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath());
            File samtoolsIndexOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                    ".bam", ".bai"));
            samtoolsIndexJob.addArgument(SAMToolsIndexCLI.OUTPUT, samtoolsIndexOutput.getAbsolutePath());
            graph.addVertex(samtoolsIndexJob);
            graph.addEdge(picardMarkDuplicatesJob, samtoolsIndexJob);

            // new job
            CondorJob samtoolsFlagstatJob = WorkflowJobFactory.createJob(++count, SAMToolsFlagstatCLI.class,
                    getWorkflowPlan());
            samtoolsFlagstatJob.setSiteName(siteName);
            samtoolsFlagstatJob.addArgument(SAMToolsFlagstatCLI.INPUT, picardMarkDuplicatesOutput.getAbsolutePath());
            File samtoolsFlagstatOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                    ".bam", ".flagstat"));
            samtoolsFlagstatJob.addArgument(SAMToolsFlagstatCLI.OUTPUT, samtoolsFlagstatOutput.getAbsolutePath());
            graph.addVertex(samtoolsFlagstatJob);
            graph.addEdge(samtoolsIndexJob, samtoolsFlagstatJob);

            CondorJob unifiedGenotyperJob = WorkflowJobFactory.createJob(++count, GATKUnifiedGenotyperCLI.class,
                    getWorkflowPlan());
            unifiedGenotyperJob.setSiteName(siteName);
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.INPUTFILE,
                    picardMarkDuplicatesOutput.getAbsolutePath());
            File unifiedGenotyperOutput = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName().replace(
                    ".bam", ".vcf"));
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.OUT, unifiedGenotyperOutput.getAbsolutePath());
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.KEY, GATKKey);
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.INTERVALS, unifiedGenotyperIntervalList);
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.REFERENCESEQUENCE, referenceSequence);
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DBSNP, unifiedGenotyperDBSNP);
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString());
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLINGTYPE,
                    GATKDownsamplingType.NONE.toString());
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.GENOTYPELIKELIHOODSMODEL, "BOTH");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.OUTPUTMODE, "EMIT_ALL_SITES");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "AlleleBalance");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "DepthOfCoverage");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HomopolymerRun");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "MappingQualityZero");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "QualByDepth");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "RMSMappingQuality");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.ANNOTATION, "HaplotypeScore");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.DOWNSAMPLETOCOVERAGE, "250");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.STANDCALLCONF, "4");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.STANDEMITCONF, "0");
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.NUMTHREADS, "4");
            unifiedGenotyperJob.setNumberOfProcessors(4);
            File unifiedGenotyperMetrics = new File(subjectFinalOutputDir, picardMarkDuplicatesOutput.getName()
                    .replace(".bam", ".metrics"));
            unifiedGenotyperJob.addArgument(GATKUnifiedGenotyperCLI.METRICS, unifiedGenotyperMetrics.getAbsolutePath());
            graph.addVertex(unifiedGenotyperJob);
            graph.addEdge(samtoolsIndexJob, unifiedGenotyperJob);

            // new job
            CondorJob calculateMaximumLikelihoodsFromVCFJob = WorkflowJobFactory.createJob(++count,
                    CalculateMaximumLikelihoodFromVCFCLI.class, getWorkflowPlan());
            calculateMaximumLikelihoodsFromVCFJob.setSiteName(siteName);
            calculateMaximumLikelihoodsFromVCFJob.addArgument(CalculateMaximumLikelihoodFromVCFCLI.VCF,
                    unifiedGenotyperOutput.getAbsolutePath());
            calculateMaximumLikelihoodsFromVCFJob.addArgument(CalculateMaximumLikelihoodFromVCFCLI.INTERVALLIST,
                    idCheckIntervalList);
            calculateMaximumLikelihoodsFromVCFJob.addArgument(CalculateMaximumLikelihoodFromVCFCLI.SAMPLE,
                    unifiedGenotyperOutput.getName().replace(".vcf", ""));
            calculateMaximumLikelihoodsFromVCFJob.addArgument(CalculateMaximumLikelihoodFromVCFCLI.ECDATA,
                    idCheckExomeChipData);
            calculateMaximumLikelihoodsFromVCFJob.addArgument(CalculateMaximumLikelihoodFromVCFCLI.OUTPUT,
                    subjectFinalOutputDir.getAbsolutePath());
            graph.addVertex(calculateMaximumLikelihoodsFromVCFJob);
            graph.addEdge(unifiedGenotyperJob, calculateMaximumLikelihoodsFromVCFJob);

            // new job
            CondorJob removeJob = WorkflowJobFactory.createJob(++count, RemoveCLI.class, getWorkflowPlan());
            removeJob.setSiteName(siteName);
            removeJob.addArgument(RemoveCLI.FILE, mergeBAMFilesOut.getAbsolutePath());
            removeJob.addArgument(RemoveCLI.FILE, picardAddOrReplaceReadGroupsOut.getAbsolutePath());
            graph.addVertex(removeJob);
            graph.addEdge(calculateMaximumLikelihoodsFromVCFJob, removeJob);

        } catch (Exception e) {
            throw new WorkflowException(e);
        }

        return graph;
    }
}
