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
        .with("null", null);
    
    final Properties props = builder.build();
    assertEquals("bar", props.get("foo"));
    assertEquals(props.toString(), builder.toString());
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
