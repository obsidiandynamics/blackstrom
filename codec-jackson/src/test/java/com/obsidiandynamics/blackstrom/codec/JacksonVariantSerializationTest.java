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
import com.fasterxml.jackson.databind.node.*;
import com.obsidiandynamics.zerolog.*;

public final class JacksonVariantSerializationTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static ObjectMapper createObjectMapper() {
    return new ObjectMapper().registerModule(new JacksonVariantModule());
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
  
  private static UniVariant emulatePacked(ObjectMapper mapper, String contentType, int contentVersion, Object content) {
    final var packed = mapper.<JsonNode>valueToTree(content);
    final var parser = new TreeTraversingParser(packed);
    return new UniVariant(new ContentHandle(contentType, contentVersion), new JacksonPackedForm(parser, packed), null);
  }
  
  private static UniVariant prepare(String contentType, int contentVersion, Object content) {
    return new UniVariant(new ContentHandle(contentType, contentVersion), null, content);
  }
  
  private static void assertPackedNode(JsonNode expectedNode, UniVariant v) {
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
  
  private static void assertUnpacked(Object expected, UniVariant v) {
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
  public void testUniVariant_prepackedScalar_failWithUnsupportedPackedForm() throws IOException {
    final var p = new UniVariant(new ContentHandle("test/scalar", 1), new IdentityPackedForm("scalar"), null);
    
    Assertions.assertThatThrownBy(() -> {
      mapper.writeValueAsString(p);
    })
    .isInstanceOf(JsonMappingException.class)
    .hasCauseInstanceOf(IllegalStateException.class)
    .hasMessage("Unsupported packed form: IdentityPackedForm");
  }
  
  @Test
  public void testUniVariant_prepackedScalar() throws IOException {
    final var p = emulatePacked(mapper, "test/scalar", 1, "scalar");
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(text("scalar"), d);
    assertUnpacked("scalar", d);
  }
  
  @Test
  public void testUniVariant_serializeScalar() throws IOException {
    final var p = prepare("test/scalar", 1, "scalar");
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(text("scalar"), d);
    assertUnpacked("scalar", d);
  }

  @Test
  public void testUniVariant_prepackedArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = emulatePacked(mapper, "test/array", 1, array);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(array(number(0), number(1), number(2)), d);
    assertUnpacked(array, d);
  }

  @Test
  public void testUniVariant_serializeArray() throws IOException {
    final var array = new int[] {0, 1, 2};
    final var p = prepare("test/array", 1, array);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(array(number(0), number(1), number(2)), d);
    assertUnpacked(array, d);
  }

  @Test
  public void testUniVariant_prepackedMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", Arrays.asList("w", "x"));
    map.put("b", Arrays.asList("y", "z"));
    final var p = emulatePacked(mapper, "test/map", 1, map);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(object(field("a", array(text("w"), text("x"))), field("b", array(text("y"), text("z")))), d);
    assertUnpacked(map, d);
  }

  @Test
  public void testUniVariant_serializeMap() throws IOException {
    final var map = new TreeMap<String, List<String>>();
    map.put("a", Arrays.asList("w", "x"));
    map.put("b", Arrays.asList("y", "z"));
    final var p = prepare("test/map", 1, map);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(object(field("a", array(text("w"), text("x"))), field("b", array(text("y"), text("z")))), d);
    assertUnpacked(map, d);
  }

  @Test
  public void testUniVariant_prepackedObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = emulatePacked(mapper, "test/obj", 1, obj);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }

  @Test
  public void testUniVariant_serializeObject() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = prepare("test/obj", 1, obj);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = mapper.readValue(encoded, UniVariant.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }

  @Test
  public void testUniVariant_serializeObject_readInterfaceType() throws IOException {
    final var obj = new TestClass("someString", 42);
    final var p = prepare("test/obj", 1, obj);
    
    final var encoded = mapper.writeValueAsString(p);
    logEncoded(encoded);
    
    final var d = (UniVariant) mapper.readValue(encoded, Variant.class);
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj, d);
  }

  @Test
  public void testMultiVariant_serializeObject() throws IOException {
    final var obj0 = new TestClass("someString", 42);
    final var obj1 = new TestClass("someOtherString", 83);
    final var p0 = prepare("test/obj-0", 1, obj0);
    final var p1 = prepare("test/obj-1", 1, obj1);
    final var mp = new MultiVariant(new UniVariant[] {p0, p1});
    
    final var encoded = mapper.writeValueAsString(mp);
    logEncoded(encoded);
    
    final var md = mapper.readValue(encoded, MultiVariant.class);
    assertEquals(2, md.getVariants().length);
    
    final var d0 = md.getVariants()[0];
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d0);
    assertUnpacked(obj0, d0);
    
    final var d1 = md.getVariants()[1];
    assertPackedNode(object(field("a", text("someOtherString")), field("b", number(83))), d1);
    assertUnpacked(obj1, d1);
  }

  @Test
  public void testMultiVariant_serializeObject_readInterfaceType() throws IOException {
    final var obj0 = new TestClass("someString", 42);
    final var p0 = prepare("test/obj-0", 1, obj0);
    final var mp = new MultiVariant(new UniVariant[] {p0});
    
    final var encoded = mapper.writeValueAsString(mp);
    logEncoded(encoded);
    
    final var md = (MultiVariant) mapper.readValue(encoded, Variant.class);
    assertEquals(1, md.getVariants().length);
    final var d = md.getVariants()[0];
    assertPackedNode(object(field("a", text("someString")), field("b", number(42))), d);
    assertUnpacked(obj0, d);
  }
}
