package edu.unc.mapseq.messaging;

import java.util.Arrays;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

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

            JSONObject parentJSONObject = new JSONObject();
            parentJSONObject.put("account_name", "rc_renci.svc");
            JSONArray entityArray = new JSONArray();

            JSONObject entityType = null;

            List<Long> idList = Arrays.asList(87443L, 87444L, 87445L, 87446L, 138601L, 138627L, 645578L, 1187302L,
                    1188124L);

            for (Long id : idList) {
                entityType = new JSONObject();
                entityType.put("entity_type", "HTSFSample");
                entityType.put("guid", id);
                entityArray.put(entityType);
            }

            entityType = new JSONObject();
            entityType.put("entity_type", "WorkflowRun");
            entityType.put("name", "merge-bam-test-jdr-1");
            entityArray.put(entityType);
            parentJSONObject.put("entities", entityArray);

            System.out.println(parentJSONObject.toString());
            producer.send(session.createTextMessage(parentJSONObject.toString()));
        } catch (JSONException | JMSException e) {
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
