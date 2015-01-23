package edu.unc.mapseq.ws.nec.mergebam;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.PropertyException;
import javax.xml.namespace.QName;
import javax.xml.ws.Service;
import javax.xml.ws.soap.SOAPBinding;

import org.junit.Test;

public class NECMergeBAMServiceTest {

    @Test
    public void testService() {
        QName serviceQName = new QName("http://mergebam.nec.ws.mapseq.unc.edu", "NECMergeBAMService");
        QName portQName = new QName("http://mergebam.nec.ws.mapseq.unc.edu", "NECMergeBAMPort");
        Service service = Service.create(serviceQName);
        String host = "152.19.198.146";
        service.addPort(portQName, SOAPBinding.SOAP11HTTP_MTOM_BINDING,
                String.format("http://%s:%d/cxf/NECMergeBAMService", host, 8181));
        NECMergeBAMService mergeBAMService = service.getPort(NECMergeBAMService.class);
        NECMergeBAMInfo results = mergeBAMService.lookupQuantificationResults("012375Sb");
        try {
            JAXBContext context = JAXBContext.newInstance(NECMergeBAMInfo.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            File resultsFile = new File("/tmp", "results.xml");
            FileWriter fw = new FileWriter(resultsFile);
            m.marshal(results, fw);
        } catch (PropertyException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
