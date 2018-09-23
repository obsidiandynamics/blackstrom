package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

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
    assertEquals(50, config.getMaxConsumerPipeYields());
    assertNotNull(config.getZlg());
    assertEquals(10, config.getIORetries());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testFluent() {
    final KafkaLedgerConfig config = new KafkaLedgerConfig()
        .withKafka(new MockKafka<>())
        .withTopic("test")
        .withProducerPipeConfig(new ProducerPipeConfig())
        .withConsumerPipeConfig(new ConsumerPipeConfig())
        .withMaxConsumerPipeYields(50)
        .withZlg(Zlg.forDeclaringClass().get())
        .withIORetries(5)
        .withDrainConfirmations(true)
        .withPrintConfig(true);
    
    assertNotNull(config.getKafka());
    assertEquals("test", config.getTopic());
    assertNotNull(config.getProducerPipeConfig());
    assertNotNull(config.getConsumerPipeConfig());
    assertEquals(50, config.getMaxConsumerPipeYields());
    assertNotNull(config.getZlg());
    assertEquals(5, config.getIORetries());
    assertTrue(config.isDrainConfirmations());
    assertTrue(config.isPrintConfig());
  }
}
