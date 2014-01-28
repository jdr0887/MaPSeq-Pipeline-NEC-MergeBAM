package edu.unc.mapseq.ws.nec.mergebam;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.jws.soap.SOAPBinding.Style;
import javax.jws.soap.SOAPBinding.Use;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.ws.BindingType;

@BindingType(value = javax.xml.ws.soap.SOAPBinding.SOAP11HTTP_BINDING)
@WebService(targetNamespace = "http://mergebam.nec.ws.mapseq.unc.edu", serviceName = "NECMergeBAMService", portName = "NECMergeBAMPort")
@SOAPBinding(style = Style.DOCUMENT, use = Use.LITERAL, parameterStyle = SOAPBinding.ParameterStyle.WRAPPED)
@Path("/NECMergeBAMService/")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public interface NECMergeBAMService {

    @GET
    @Path("/lookupQuantificationResults/{subject}")
    @WebMethod
    public NECMergeBAMInfo lookupQuantificationResults(
            @PathParam("subject") @WebParam(name = "subject") String subject);

}
