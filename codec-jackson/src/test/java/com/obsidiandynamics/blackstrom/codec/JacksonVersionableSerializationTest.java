package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;
import static java.util.Arrays.*;
import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.assertj.core.api.*;
import org.junit.*;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.*;
import com.fasterxml.jackson.databind.node.*;
import com.obsidiandynamics.zerolog.*;

public final class JacksonVersionableSerializationTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static ObjectMapper createObjectMapper() {
    final var mapper = new ObjectMapper();
    final var m = new SimpleModule();
    m.addSerializer(Versionable.class, new JacksonVersionableSerializer());
    m.addDeserializer(Versionable.class, new JacksonVersionableDeserializer());
    mapper.registerModule(m);
    return mapper;
  }
  
  private static void logEncoded(String encoded) {
    zlg.t("encoded %s", z -> z.arg(encoded));
  }
  
  private static final class TestClass {
    @JsonProperty
    private final String a;

    @JsonProperty
    private final int b;
    
    TestClass(@JsonProperty("a") String a, @JsonProperty("b") int b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public int hashCode() {
      final var prime = 31;
      var result = 1;
      result = prime * result + Objects.hashCode(a);
      result = prime * result + b;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof TestClass) {
        final var that = (TestClass) obj;
        return Objects.equals(a, that.a) && b == that.b;
      } else {
        return false;
      }
    }
  }
  
  private static Versionable emulatePacked(ObjectMapper mapper, String contentType, int contentVersion, Object content) {
    final var packed = mapper.<JsonNode>valueToTree(content);
    final var parser = new TreeTraversingParser(packed);
    return new Versionable(new ContentHandle(contentType, contentVersion), new JacksonPackedForm(parser, packed), null);
  }
  
  private static Versionable pack(String contentType, int contentVersion, Object content) {
    return new Versionable(new ContentHandle(contentType, contentVersion), null, content);
  }
  
  private static void assertPackedNode(JsonNode expectedNode, Versionable v) {
    final var packedForm = mustBeSubtype(v.getPacked(), JacksonPackedForm.class, AssertionError::new);
    assertEquals(expectedNode, packedForm.getNode());
  }
  
  private static TextNode text(String s) {
    return JsonNodeFactory.instance.textNode(s);
  }
  
  private static NumericNode number(int v) {
    return JsonNodeFactory.instance.numberNode(v);
  }
  
  private static ArrayNode array(JsonNode... childNodes) {
    return JsonNodeFactory.instance.arrayNode().addAll(asList(childNodes));
  }
  
  private static ObjectNode object(Field... fields) {
    final var node = JsonNodeFactory.instance.objectNode();
    for (var kv : fields) {
      node.set(kv.name, kv.value);
    }
    return node;
  }
  
  private static Field field(String name, JsonNode value)  {
    return new Field(name, value);
  }
  
  private static final class Field {
    final String name;
    final JsonNode value;
    
    Field(String name, JsonNode value) {
      this.name = name;
      this.value = value;
    }
  }
  
  private static void assertUnpacked(Object expected, Versionable v) {
    final var packed = mustBeSubtype(v.getPacked(), JacksonPackedForm.class, AssertionError::new);
    final var unpacked = JacksonUnpacker.getInstance().unpack(packed, expected.getClass());
    assertTrue(expected + " != " + unpacked, Objects.deepEquals(expected, unpacked));
  }
  
  private ObjectMapper mapper;
  
  @Before
  public void before() {
    mapper = createObjectMapper();
  }
  
  @Test
  public void testPrepackedScalar_failWithUnsupportedPackedForm() throws IOException {
    final var p = new Versionable(new ContentHandle("test/scalar", 1), new IdentityPackedForm("scalar"), null);
    
    Assertions.assertThatThrownBy(() -> {
      mapper.writeValueAsString(p);
    })
    .isInstanceOf(JsonMappingException.class)
    .hasCauseInstanceOf(IllegalStateException.class)
    .hasMessage("Unsupported packed form: IdentityPackedForm");
  }
  
  @Test
  public void testPrepackedScalar() throws IOException {
    final var p = emulatePacked(mapper, "test/scalar", 1, "scalar");
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(text("scalar"), d);
    assertUnpacked("scalar", d);
  }
  
  @Test
  public void testSerializeScalar() throws IOException {
    final var p = pack("test/scalar", 1, "scalar");
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(text("scalar"), d);
    assertUnpacked("scalar", d);
  }

  @Test
  public void testPrepackedArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = emulatePacked(mapper, "test/array", 1, array);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(array(number(0), number(1), number(2)), d);
    assertUnpacked(array, d);
  }

  @Test
  public void testSerializeArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = pack("test/array", 1, array);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(array(number(0), number(1), number(2)), d);
    assertUnpacked(array, d);
  }

  @Test
  public void testPrepackedMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", Arrays.asList("w", "x"));
    map.put("b", Arrays.asList("y", "z"));
    final var p = emulatePacked(mapper, "test/map", 1, map);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(object(field("a", array(text("w"), text("x"))), field("b", array(text("y"), text("z")))), d);
    assertUnpacked(map, d);
  }

  @Test
  public void testSerializeMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", Arrays.asList("w", "x"));
    map.put("b", Arrays.asList("y", "z"));
    final var p = pack("test/map", 1, map);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(object(field("a", array(text("w"), text("x"))), field("b", array(text("y"), text("z")))), d);
    assertUnpacked(map, d);
  }

  @Test
  public void testPrepackedObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = emulatePacked(mapper, "test/obj", 1, obj);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }

  @Test
  public void testSerializeObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = pack("test/obj", 1, obj);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, Versionable.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }
}
