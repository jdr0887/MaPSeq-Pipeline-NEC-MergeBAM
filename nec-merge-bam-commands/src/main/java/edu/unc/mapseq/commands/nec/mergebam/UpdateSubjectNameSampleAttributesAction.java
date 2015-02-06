package edu.unc.mapseq.commands.nec.mergebam;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.commands.Option;
import org.apache.karaf.shell.console.AbstractAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.FlowcellDAO;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;

@Command(scope = "nec-mergebam", name = "update-subject-name-sample-attributes", description = "Update subjectName Sample Attributes")
public class UpdateSubjectNameSampleAttributesAction extends AbstractAction {

    private final Logger logger = LoggerFactory.getLogger(UpdateSubjectNameSampleAttributesAction.class);

    @Option(name = "--dryRun", description = "Don't persist anything", required = false, multiValued = false)
    private Boolean dryRun = Boolean.FALSE;

    private MaPSeqDAOBean maPSeqDAOBean;

    public UpdateSubjectNameSampleAttributesAction() {
        super();
    }

    @Override
    public Object doExecute() {

        FlowcellDAO flowcellDAO = maPSeqDAOBean.getFlowcellDAO();
        SampleDAO sampleDAO = maPSeqDAOBean.getSampleDAO();

        try {

            List<String> lines = IOUtils.readLines(getClass().getClassLoader().getResourceAsStream(
                    "edu/unc/mapseq/commands/nec/mergebam/SubjectToBarcodeMap.csv"));

            List<Flowcell> flowcells = flowcellDAO.findByStudyName("NEC");

            if (flowcells != null && !flowcells.isEmpty()) {

                for (Flowcell flowcell : flowcells) {

                    logger.info(flowcell.toString());

                    List<Sample> samples = sampleDAO.findByFlowcellId(flowcell.getId());

                    if (samples != null && !samples.isEmpty()) {

                        for (Sample sample : samples) {

                            if ("Undetermined".equals(sample.getBarcode())) {
                                continue;
                            }

                            logger.info(sample.toString());

                            String subjectName = null;

                            // find subjectName
                            for (String line : lines) {
                                if (line.contains(flowcell.getName()) && line.contains(sample.getName())) {
                                    subjectName = line.split(",")[0];
                                    break;
                                }
                            }

                            if (StringUtils.isEmpty(subjectName)) {
                                continue;
                            }

                            logger.info("subjectName: {}", subjectName);

                            if (!dryRun) {

                                Set<Attribute> attributes = sample.getAttributes();

                                Set<String> attributeNameSet = new HashSet<String>();
                                for (Attribute attribute : attributes) {
                                    attributeNameSet.add(attribute.getName());
                                }

                                if (!attributeNameSet.contains("subjectName")) {
                                    Attribute attribute = new Attribute("subjectName", subjectName);
                                    attributes.add(attribute);
                                    maPSeqDAOBean.getSampleDAO().save(sample);
                                } else {
                                    for (Attribute attribute : attributes) {
                                        if ("subjectName".equals(attribute.getName())) {
                                            attribute.setValue(subjectName);
                                            maPSeqDAOBean.getAttributeDAO().save(attribute);
                                            break;
                                        }
                                    }
                                }

                            }

                        }

                    }

                }

            }

        } catch (MaPSeqDAOException | IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public Boolean getDryRun() {
        return dryRun;
    }

    public void setDryRun(Boolean dryRun) {
        this.dryRun = dryRun;
    }

    public MaPSeqDAOBean getMaPSeqDAOBean() {
        return maPSeqDAOBean;
    }

    public void setMaPSeqDAOBean(MaPSeqDAOBean maPSeqDAOBean) {
        this.maPSeqDAOBean = maPSeqDAOBean;
    }

}
