package edu.unc.mapseq.ws.nec.mergebam.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.ws.nec.mergebam.NECMergeBAMInfo;
import edu.unc.mapseq.ws.nec.mergebam.NECMergeBAMService;

public class NECMergeBAMServiceImpl implements NECMergeBAMService {

    private final Logger logger = LoggerFactory.getLogger(NECMergeBAMServiceImpl.class);

    @Override
    public NECMergeBAMInfo lookupQuantificationResults(String subject) {
        logger.debug("ENTERING lookupQuantificationResults(String)");

        NECMergeBAMInfo ret = new NECMergeBAMInfo();

        File subjectFinalDir = new File(String.format("/proj/renci/sequence_analysis/NEC/subjects/%s/final", subject));
        logger.debug("subjectFinalDir file is: {}", subjectFinalDir.getAbsolutePath());

        if (!subjectFinalDir.exists()) {
            logger.warn("subjectFinalDir file doesn't exist");
            return ret;
        }

        File flagstatFile = new File(subjectFinalDir, String.format("%s.merged.rg.deduped.flagstat", subject));
        logger.debug("flagstat file is: {}", flagstatFile.getAbsolutePath());

        if (!flagstatFile.exists()) {
            logger.warn("flagstat file doesn't exist");
            return ret;
        }

        try {
            List<String> lines = FileUtils.readLines(flagstatFile);

            for (String line : lines) {

                if (line.contains("in total")) {
                    String value = line.substring(0, line.indexOf(" ")).trim();
                    try {
                        ret.setPassedReads(Integer.valueOf(value));
                    } catch (Exception e) {
                        logger.error("problem getting passedReads, value: {}", value);
                    }
                }

                if (line.contains("mapped (")) {
                    Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.matches()) {
                        String value = matcher.group(1);
                        value = value.substring(0, value.indexOf("%")).trim();
                        if (StringUtils.isNotEmpty(value)) {
                            try {
                                ret.setAligned(Float.valueOf(value));
                            } catch (Exception e) {
                                logger.error("problem getting mapped, value: {}", value);
                            }
                        }
                    }
                }

                if (line.contains("properly paired (")) {
                    Pattern pattern = Pattern.compile("^.+\\((.+)\\)");
                    Matcher matcher = pattern.matcher(line);
                    if (matcher.matches()) {
                        String value = matcher.group(1);
                        value = value.substring(0, value.indexOf("%"));
                        if (StringUtils.isNotEmpty(value)) {
                            try {
                                ret.setPaired(Float.valueOf(value));
                            } catch (Exception e) {
                                logger.error("problem getting paired, value: {}", value);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        File compareExpectedOutput = new File(subjectFinalDir, String.format("%s.merged.rg.deduped.ec.tsv", subject));
        logger.debug("compareExpectedOutput file is: {}", compareExpectedOutput.getAbsolutePath());

        if (!compareExpectedOutput.exists()) {
            logger.warn("compareExpectedOutput file doesn't exist");
            return ret;
        }

        try {
            List<String> lines = FileUtils.readLines(compareExpectedOutput);
            if (lines != null && lines.size() > 1) {
                String line = lines.get(1);
                ret.setBestMatch(line.split("\\t")[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ret;
    }

}
