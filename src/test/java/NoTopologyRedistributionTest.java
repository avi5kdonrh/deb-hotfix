import jakarta.jms.*;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.cluster.impl.MessageLoadBalancingType;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class NoTopologyRedistributionTest extends ActiveMQTestBase implements MessageListener{


   ActiveMQServer server1;
   ActiveMQServer server2;
   CountDownLatch countDownLatch = new CountDownLatch(1);
   private static final String address = "Address";
   private static final String queue = "Queue";
   private static final String fqqn = address + "::" + queue;
   public void startServer() throws Exception {

   }

   @BeforeClass
   public static void beforeClass() throws Exception {
     // Assume.assumeTrue(CheckLeak.isLoaded());
   }

   @Override
   @Before
   public void setUp() throws Exception {
      startServer();
   }

   @Override
   public void tearDown() throws Exception {
      super.tearDown();
      server1 = null;
      server2 = null;
   }

   @Test
   public void testRedistributor() throws Exception {

      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put("host", "localhost");
      map.put("port", 61616);
      HashMap<String, Object> map2 = new HashMap<String, Object>();
      map2.put("host", "localhost");
      map2.put("port", 61617);

      ClusterConnectionConfiguration clusterConnectionConfiguration = new ClusterConnectionConfiguration().
              setName("my-cluster").setAddress(address).setConnectorName("local").
              setInitialConnectAttempts(10).
              setRetryInterval(100).setDuplicateDetection(true).setMaxHops(1).
              setProducerWindowSize(0).setMessageLoadBalancingType(MessageLoadBalancingType.ON_DEMAND).
              setStaticConnectors(List.of("remote"));


      ConfigurationImpl config1 =  createBasicConfig(0);
      config1.getConnectorConfigurations().put("local", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map));
      config1.getConnectorConfigurations().put("remote", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map2));
      config1.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY,map));

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedistributionDelay(0);
      addressSettings.setAutoCreateAddresses(true);
      addressSettings.setAutoDeleteAddresses(true);
      addressSettings.setAutoCreateQueues(true);
      addressSettings.setAutoDeleteQueues(true);

      server1 = createServer(false, config1);
      server1.getConfiguration().addAddressSetting("#", addressSettings);
      server1.getConfiguration().addClusterConfiguration(clusterConnectionConfiguration);
      server1.getConfiguration().setClusterUser("admin");
      server1.getConfiguration().setClusterPassword("admin");
      server1.start();


      ConfigurationImpl config2 =  createBasicConfig(0);
      config2.getConnectorConfigurations().put("remote", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map));
      config2.getConnectorConfigurations().put("local", new TransportConfiguration(NETTY_CONNECTOR_FACTORY,map2));
      config2.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY,map2));


      server2 = createServer(false, config2);
      server2.getConfiguration().addAddressSetting("#", addressSettings);
      server2.getConfiguration().addClusterConfiguration(clusterConnectionConfiguration);
      server2.getConfiguration().setClusterUser("admin");
      server2.getConfiguration().setClusterPassword("admin");
      server2.start();


      Thread.sleep(4000);
      ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
      Connection connection = activeMQConnectionFactory.createConnection();
      connection.start();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      session.createConsumer(session.createQueue(fqqn)).setMessageListener(this);
      System.out.println(">>>>>>>>>>>>>> Consumer Created >>>>>>>>>>>>>>");



      ConnectionFactory factory1 = CFUtil.createConnectionFactory("core", "tcp://localhost:61617");
      try (Connection connection1 = factory1.createConnection()) {
         connection1.start();
         Session session1= connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Destination jmsQueue = session1.createQueue(fqqn);
         session1.createProducer(jmsQueue).send(session1.createTextMessage("test"));
         System.out.println(">> SENT >>>>>>>>>>>>>>>>>>>");
      }

      Thread.sleep(5000);
      //TODO if you uncomment the following line, the async consumer will receive the message
     // System.out.println(session.createConsumer(session.createQueue(fqqn)).receive(1));
      boolean result = countDownLatch.await(5, TimeUnit.SECONDS);
      assertTrue(result);
      server1.stop();
      server2.stop();
      super.tearDown();

      }

   @Override
   public void onMessage(Message message) {
      System.out.println(">> Async Consumer Received The Message >> "+message);
      countDownLatch.countDown();
   }
}
