package com.obsidiandynamics.blackstrom.ledger;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.blackstrom.codec.*;
import com.obsidiandynamics.blackstrom.spotter.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaLedgerConfigTest {
  @Test
  public void testConfig() throws IOException {
    final var config = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(KafkaLedgerConfigTest.class.getClassLoader().getResourceAsStream("kafkaledger.conf"))
        .map(KafkaLedgerConfig.class);
    
    config.validate();
    assertNotNull(config.getKafka());
    assertEquals(MockKafka.class, config.getKafka().getClass());
    assertEquals("test", config.getTopic());
    assertNotNull(config.getCodec());
    assertEquals(KryoMessageCodec.class, config.getCodec().getClass());
    assertNotNull(config.getProducerPipeConfig());
    assertNotNull(config.getConsumerPipeConfig());
    assertEquals(50, config.getMaxConsumerPipeYields());
    assertEquals(1000, config.getPollTimeout());
    assertNotNull(config.getZlg());
    assertEquals(5, config.getIoAttempts());
    assertTrue(config.isDrainConfirmations());
    assertEquals(60_000, config.getDrainConfirmationsTimeout());
    assertEquals(60_000, config.getSpotterConfig().getTimeout());
    assertEquals(20_000, config.getSpotterConfig().getGracePeriod());
    assertTrue(config.isEnforceMonotonicity());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testFluent() {
    final var codec = new IdentityMessageCodec();
    final var config = new KafkaLedgerConfig()
        .withKafka(new MockKafka<>())
        .withTopic("test")
        .withCodec(codec)
        .withProducerPipeConfig(new ProducerPipeConfig())
        .withConsumerPipeConfig(new ConsumerPipeConfig())
        .withMaxConsumerPipeYields(50)
        .withPollTimeout(1000)
        .withZlg(Zlg.forDeclaringClass().get())
        .withIoAttempts(5)
        .withDrainConfirmations(true)
        .withDrainConfirmationsTimeout(60_000)
        .withEnforceMonotonicity(true)
        .withPrintConfig(true)
        .withSpotterConfig(new SpotterConfig().withTimeout(60_000));
    config.validate();
    
    assertNotNull(config.getKafka());
    assertEquals("test", config.getTopic());
    assertSame(codec, config.getCodec());
    assertNotNull(config.getProducerPipeConfig());
    assertNotNull(config.getConsumerPipeConfig());
    assertEquals(50, config.getMaxConsumerPipeYields());
    assertEquals(1000, config.getPollTimeout());
    assertNotNull(config.getZlg());
    assertEquals(5, config.getIoAttempts());
    assertTrue(config.isDrainConfirmations());
    assertEquals(60_000, config.getDrainConfirmationsTimeout());
    assertTrue(config.isEnforceMonotonicity());
    assertTrue(config.isPrintConfig());
    assertEquals(60_000, config.getSpotterConfig().getTimeout());
  }
}
