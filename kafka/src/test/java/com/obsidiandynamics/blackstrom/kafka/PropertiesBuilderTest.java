package com.obsidiandynamics.blackstrom.kafka;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.*;
import java.util.*;

import org.junit.*;

import com.obsidiandynamics.yconf.*;

public final class PropertiesBuilderTest {
  @Test
  public void testApi() {
    final PropertiesBuilder builder = new PropertiesBuilder()
        .with("foo", "bar")
        .with("null", null)
        .withSystemDefault("amount", 100);
    
    final Properties props = builder.build();
    assertEquals("bar", props.get("foo"));
    assertEquals(props.toString(), builder.toString());
    assertEquals("100", props.getProperty("amount"));
  }
  
  @Test
  public void testDefault() {
    final Properties defaults = new PropertiesBuilder()
        .with("amount", 100)
        .build();
    
    final Properties props = new PropertiesBuilder()
        .withDefault("amount", defaults, 200)
        .withDefault("amountUseSupplied", defaults, 300)
        .withDefault("amountUseNull", defaults, null)
        .build();

    assertEquals("100", props.getProperty("amount"));
    assertEquals("300", props.getProperty("amountUseSupplied"));
    assertEquals(null, props.getProperty("amountUseNull"));
  }
  
  @Test
  public void testConfig() throws IOException {
    final PropertiesBuilder builder = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(PropertiesBuilderTest.class.getClassLoader().getResourceAsStream("propertiesbuilder.conf"))
        .map(PropertiesBuilder.class);
    assertNotNull(builder);
    assertEquals(new PropertiesBuilder()
                 .with("a", "A")
                 .with("b", "B")
                 .with("c", "C")
                 .build(),
                 builder.build());
  }
}
