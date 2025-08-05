import javax.jms.*;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.cli.Artemis;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.address.CreateAddress;
import org.apache.activemq.artemis.cli.commands.queue.CreateQueue;
import org.apache.activemq.artemis.cli.commands.queue.StatQueue;
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

public class ConsumerBeforeTopologyFailTest extends ActiveMQTestBase implements MessageListener{


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

      String url1 = "tcp://localhost:61616";
      String url2 = "tcp://localhost:61617";

      ClusterConnectionConfiguration clusterConnectionConfiguration = new ClusterConnectionConfiguration().
              setName("my-cluster").setAddress(address).setConnectorName("local").
              setInitialConnectAttempts(10).
              setRetryInterval(100).setDuplicateDetection(true).setMaxHops(1).
              setMessageLoadBalancingType(MessageLoadBalancingType.ON_DEMAND).
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

      createQueue(url1);
      System.out.println(">>>> QUEUE STAT AFTER CREATING QUEUE >>>");
      stats(url1);

      Thread.sleep(1000);

         ActiveMQConnectionFactory factoryBroker1 = new ActiveMQConnectionFactory(url1);
         Connection connectionBroker1 = factoryBroker1.createConnection();
         connectionBroker1.start();
         Session sessionBroker1 = connectionBroker1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         sessionBroker1.createConsumer(sessionBroker1.createQueue(fqqn)).setMessageListener(this);


      System.out.println(">>>>>>>>>>>>>> Consumer Created >>>>>>>>>>>>>>");
      System.out.println(">>>> QUEUE STAT AFTER CREATING ASYNC CONSUMER >>>");
      stats(url1);

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

      createQueue(url2);
      System.out.println(">>>> QUEUE STAT AFTER CREATING QUEUE ON ANOTHER NODE >>>");
      stats(url2);


     ActiveMQConnectionFactory factoryBroker2 = new ActiveMQConnectionFactory(url2);
         Connection connectionBroker2 = factoryBroker2.createConnection();
           connectionBroker2.start();
           Session sessionBroker2 = connectionBroker2.createSession(false, Session.AUTO_ACKNOWLEDGE);
           Destination jmsQueue = sessionBroker2.createQueue(fqqn);
           sessionBroker2.createProducer(jmsQueue).send(sessionBroker2.createTextMessage("test"));
           System.out.println(">> SENT >>>>>>>>>>>>>>>>>>>");

      System.out.println(">>>> QUEUE STAT AFTER SENDING MESSAGE ON ANOTHER NODE >>>");
     stats(url2);


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

   private void createQueue(String url) throws Exception {
      Artemis.buildCommand(true, true, true)
              .execute("queue", "create", "--url="+url,
                      "--name="+queue,
                      "--address="+address,
                      "--anycast",
                      "--durable",
                      "--purge-on-no-consumers=false",
                      "--auto-create-address=true");
   }

   private void stats(String url) throws Exception {
      Artemis.buildCommand(true, true, true)
              .execute("queue", "stat", "--url="+url,
                      "--clustered",
                      "--queueName="+queue);
   }

}
