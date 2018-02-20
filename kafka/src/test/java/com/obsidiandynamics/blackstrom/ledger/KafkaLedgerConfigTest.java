package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;
import org.mockito.*;
import org.slf4j.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.kafka.*;
import com.obsidiandynamics.yconf.*;

public final class KafkaLedgerConfigTest {
  @Test
  public void testConfig() throws IOException {
    final KafkaLedgerConfig config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(KafkaLedgerConfigTest.class.getClassLoader().getResourceAsStream("kafkaledger.conf"))
        .map(KafkaLedgerConfig.class);
    
    assertNotNull(config.getKafka());
    assertEquals(MockKafka.class, config.getKafka().getClass());
    assertEquals("test", config.getTopic());
    assertNotNull(config.getCodec());
    assertEquals(KryoMessageCodec.class, config.getCodec().getClass());
    assertNotNull(config.getProducerPipeConfig());
    assertNotNull(config.getConsumerPipeConfig());
    assertNotNull(config.getLog());
    assertEquals(10, config.getAttachRetries());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testFluent() {
    final KafkaLedgerConfig config = new KafkaLedgerConfig()
        .withKafka(new MockKafka<>())
        .withTopic("test")
        .withProducerPipeConfig(new ProducerPipeConfig())
        .withConsumerPipeConfig(new ConsumerPipeConfig())
        .withLog(Mockito.mock(Logger.class))
        .withAttachRetries(5);
    
    assertNotNull(config.getKafka());
    assertEquals("test", config.getTopic());
    assertNotNull(config.getProducerPipeConfig());
    assertNotNull(config.getConsumerPipeConfig());
    assertNotNull(config.getLog());
    assertEquals(5, config.getAttachRetries());
  }
}
