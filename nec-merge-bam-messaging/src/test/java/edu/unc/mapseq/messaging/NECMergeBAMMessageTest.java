package edu.unc.mapseq.messaging;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class NECMergeBAMMessageTest {

    @Test
    public void testCAVASAQueue() {
        // String host = "localhost";
        String host = "biodev2.its.unc.edu";
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://" + host + ":61616");

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/nec.mergebam");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeArrayFieldStart("entities");

            List<Long> idList = Arrays.asList(87443L, 87444L, 87445L, 87446L, 138601L, 138627L, 645578L, 1187302L,
                    1188124L);

            for (Long id : idList) {
                generator.writeStartObject();
                generator.writeStringField("entityType", "Sample");
                generator.writeStringField("id", id.toString());
                generator.writeEndObject();
            }

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", "merge-bam-test-jdr-1");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();

            producer.send(session.createTextMessage(sw.toString()));

        } catch (JMSException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }
}
