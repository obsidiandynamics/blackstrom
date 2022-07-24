package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.io.*;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.*;
import com.fasterxml.jackson.databind.module.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.ser.std.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.io.*;
import com.obsidiandynamics.zerolog.*;

public final class JacksonUnpackerTest extends AbstractUnpackerTest {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static void logEncoded(String encoded) {
    zlg.t("encoded %s", z -> z.arg(encoded));
  }
  
  private static final class SchemaSerializer<S extends BaseSchema> extends StdSerializer<S> {
    private static final long serialVersionUID = 1L;

    SchemaSerializer(Class<S> cls) {
      super(cls);
    }

    @Override
    public void serialize(BaseSchema s, JsonGenerator gen, SerializerProvider provider) throws IOException {
      mustBeSubtype(s, handledType(), withMessage(() -> "Not a subtype of " + handledType(), IllegalStateException::new));
      gen.writeStartObject();
      gen.writeStringField("type", s.getType());
      gen.writeStringField("value", s.getValue());
      gen.writeEndObject();
    }
  }
  
  private static final class SchemaDeserializer<S extends BaseSchema> extends StdDeserializer<S> {
    private static final long serialVersionUID = 1L;

    SchemaDeserializer(Class<S> cls) {
      super(cls);
    }

    @Override
    public S deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      final var thisNode = p.getCodec().<ObjectNode>readTree(p);
      final var type = thisNode.get("type").asText();
      final var value = thisNode.get("value").asText();
      return Exceptions.wrap(() -> {
        final var classType = Classes.<Class<S>>cast(handledType());
        return classType.getDeclaredConstructor(String.class, String.class).newInstance(type, value);
      }, RuntimeException::new);
    }
  }
  
  private ObjectMapper mapper;
  
  @Override
  protected void init() {
    final var module = new SimpleModule() {
      private static final long serialVersionUID = 1L;
      
      <S extends BaseSchema> void addSerde(Class<S> cls) {
        addSerializer(new SchemaSerializer<>(cls));
        addDeserializer(cls, new SchemaDeserializer<>(cls));
      }
    };
    module.addSerde(SchemaFoo_v0.class);
    module.addSerde(SchemaFoo_v1.class);
    module.addSerde(SchemaBar_v0.class);
    module.addSerde(SchemaBar_v1.class);
    
    mapper = new ObjectMapper()
        .registerModule(module)
        .registerModule(new JacksonVariantModule())
        .registerModule(new JacksonPayloadModule());
  }

  @Override
  protected Unpacker<?> getUnpacker() {
    return JacksonUnpacker.getInstance();
  }

  @Override
  protected <T> T roundTrip(T obj) {
    final var objClass = obj.getClass();
    return Exceptions.wrapStrict(() -> {
      final var json = mapper.writeValueAsString(obj);
      logEncoded(json);
      return Classes.cast(mapper.readValue(json, objClass));
    }, RuntimeIOException::new);
  }
}
