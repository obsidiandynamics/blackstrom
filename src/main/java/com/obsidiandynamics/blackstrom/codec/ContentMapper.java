package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.obsidiandynamics.func.*;

/**
 *  Provides functionality for encapsulating arbitrary object content into {@link Variant}
 *  containers, and mapping from a {@link Variant} back to the original object content. <p>
 *  
 *  A {@link ContentMapper} serves as a registry of {@link Unpacker} implementations 
 *  and version-to-class mappings. All operations on {@link Variant}s must be performed 
 *  via a suitably configured {@link ContentMapper}. <p>
 *  
 *  When a {@link Variant} is <em>prepared</em>, a content type and version pair
 *  (captured in a {@link ContentHandle}) is resolved for the given content object 
 *  by consulting the mappings stored herein. These are then written out as part of 
 *  the object's wire representation during the subsequent serialization process. <p>
 *  
 *  Upon deserialization, the wire form is parsed to a limited extent — capturing
 *  the inline content type and version headers, but keeping the content-specific payload
 *  elements in an intermediate {@link PackedForm} (which varies depending on the codec), 
 *  without attempting to map the serialized content back to its native Java class.
 *  The {@link PackedForm} is encapsulated inside the returned {@link UniVariant}. In
 *  the case of a {@link MultiVariant}, multiple {@link PackedForm}s may be 
 *  encapsulated. <p>
 *  
 *  When a {@link Variant} is <em>mapped</em> to a Java class following deserialization, 
 *  the {@link ContentMapper} first resolves a corresponding mapping for the
 *  stored content type and version pair, which yields the concrete class type (if a 
 *  configured mapping exists). This class type together with the {@link PackedForm} is 
 *  then handed to an appropriate {@link Unpacker}, which completes the deserialization 
 *  process and maps the packed content to its terminal Java class form. If no mapping is 
 *  configured for the packed content type and version, it is assumed that the content is 
 *  unsupported, and a {@code null} is returned. <p>
 *  
 *  A {@link Variant} may be of the type {@link MultiVariant}, which houses multiple
 *  {@link UniVariant}s. This allows the mapping process to enumerate over the variants
 *  in a fallback manner, trying the first variant then advancing to the next, until either
 *  all variants are exhausted (yielding a {@code null}) or a supported content type and
 *  version is located (yielding the reconstituted object). <p>
 *  
 *  The specifics of the (de)serialization will depend on the particular codec employed.
 *  The appropriate (de)serializers must be registered with the codec library prior to 
 *  (de)serializing {@link Variant} containers. Furthermore, the mapping process following 
 *  a deserialization requires a suitable {@link Unpacker} implementation. The latter must 
 *  be registered directly with the {@link ContentMapper} for every supported codec, as 
 *  unpacking may require capabilities beyond those natively supported by codec library. 
 *  For example, deserialization may acquire and release pooled resources, making two-pass 
 *  deserialization impossible without the special handling provided by an {@link Unpacker}. 
 */
public final class ContentMapper {
  private final Map<Class<? extends PackedForm>, Unpacker<?>> unpackers = new HashMap<>();

  private final Map<String, VersionMappings> typeToVersions = new LinkedHashMap<>();

  private final Map<Class<?>, VersionMapping> classToVersion = new HashMap<>();

  public static final class IllegalMappingException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    IllegalMappingException(String m) { super(m); }
  }

  public static final class NoSuchMappingException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    NoSuchMappingException(String m) { super(m); }
  }

  public static final class InsufficientVariantsException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    InsufficientVariantsException(String m) { super(m); }
  }

  public static final class MixedContentTypesException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    MixedContentTypesException(String m) { super(m); }
  }

  public static final class NonDecreasingContentVersionsException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    NonDecreasingContentVersionsException(String m) { super(m); }
  }

  private static final class VersionMappings {
    private final List<VersionMapping> list = new ArrayList<>();

    void add(VersionMapping mapping) {
      if (list.isEmpty()) {
        list.add(mapping);
      } else {
        final var preceding = list.get(list.size() - 1);
        final var newVersion = mapping.handle.getVersion();
        final var precedingVersion = preceding.handle.getVersion();
        mustBeGreater(newVersion, precedingVersion, 
                      withMessage(() -> 
                      "Next mapping (v" + newVersion + ") is not ahead of the preceding (v" + precedingVersion + ")", 
                      IllegalMappingException::new));
        list.add(mapping);
      }
    }

    @Override
    public String toString() {
      return list.toString();
    }

    void ensureSufficientMappings(int supplied) {
      mustBeEqual(supplied, list.size(), 
                  withMessage(() -> "Insufficient variants supplied; expected: " + list.size() + ", got: " + supplied, 
                              InsufficientVariantsException::new));
    }
  }

  private static final class VersionMapping {
    final VersionMappings mappings;

    final ContentHandle handle;

    final Class<?> contentClass;

    VersionMapping(VersionMappings mappings, ContentHandle handle, Class<?> contentClass) {
      this.mappings = mappings;
      this.handle = handle;
      this.contentClass = contentClass;
    }

    @Override
    public String toString() {
      return handle.getVersion() + " -> " + contentClass.getName();
    }
  }

  public String printSnapshots() {
    return typeToVersions.toString();
  }

  public ContentMapper withUnpacker(Unpacker<?> unpacker) {
    mustExist(unpacker, "Unpacker cannot be null");
    final var packedClass = unpacker.getPackedType();
    mustBeFalse(unpackers.containsKey(packedClass), 
                withMessage(() -> "Duplicate unpacker for class " + packedClass.getName(), IllegalArgumentException::new));
    unpackers.put(Classes.cast(packedClass), unpacker);
    return this;
  }

  public ContentMapper withSnapshot(String contentType, int contentVersion, Class<?> contentClass) {
    ContentHandle.validateContentType(contentType);
    ContentHandle.validateContentVersion(contentVersion);
    mustExist(contentClass, "Content class cannot be null");
    final var existingMapping = classToVersion.get(contentClass);
    mustBeNull(existingMapping, 
               withMessage(() -> "A mapping already exists for content " + contentClass, IllegalMappingException::new));
    final var mappings = getOrCreateVersionMappings(contentType);
    final var mapping = new VersionMapping(mappings, new ContentHandle(contentType, contentVersion), contentClass);
    mappings.add(mapping);
    classToVersion.put(contentClass, mapping);
    return this;
  }

  private VersionMapping checkedGetMapping(Class<?> cls) {
    return mustExist(classToVersion, cls, "No mapping for %s", NoSuchMappingException::new);
  }
  
  public interface Preparer {
    Variant prepare(Object content);
    
    Variant prepare(Object... contentItems);
  }

  public final class StandardRelaxedPreparer implements Preparer {
    @Override
    public UniVariant prepare(Object content) {
      mustExist(content, "Content cannot be null");
      final var mapping = checkedGetMapping(content.getClass());
      final var handle = mapping.handle;
      return new UniVariant(handle, null, content);
    }

    @Override
    public MultiVariant prepare(Object... contentItems) {
      mustExist(contentItems, "Content items cannot be null");
      mustBeGreater(contentItems.length, 0, illegalArgument("Content items cannot be empty"));
      
      final var variants = new UniVariant[contentItems.length];
      for (var i = 0; i < contentItems.length; i++) {
        variants[i] = prepare(contentItems[i]);
      }
      return new MultiVariant(variants);
    }
  }

  private final StandardRelaxedPreparer relaxedPreparer = new StandardRelaxedPreparer();

  public StandardRelaxedPreparer relaxed() { return relaxedPreparer; }
  
  public final class CompactRelaxedPreparer implements Preparer {
    @Override
    public UniVariant prepare(Object content) {
      return relaxedPreparer.prepare(content);
    }

    @Override
    public Variant prepare(Object... contentItems) {
      mustExist(contentItems, "Content items cannot be null");
      mustBeGreater(contentItems.length, 0, illegalArgument("Content items cannot be empty"));
      if (contentItems.length == 1) {
        return relaxedPreparer.prepare(contentItems[0]);
      } else {
        return relaxedPreparer.prepare(contentItems);
      }
    }
  }
  
  private final CompactRelaxedPreparer compactRelaxedPreparer = new CompactRelaxedPreparer();
  
  public CompactRelaxedPreparer compactRelaxed() { return compactRelaxedPreparer; }

  public final class StandardStrictPreparer implements Preparer {
    @Override
    public MultiVariant prepare(Object content) {
      mustExist(content, "Content cannot be null");
      final var mapping = checkedGetMapping(content.getClass());
      mapping.mappings.ensureSufficientMappings(1);
      return new MultiVariant(new UniVariant(mapping.handle, null, content));
    }

    @Override
    public MultiVariant prepare(Object... contentItems) {
      mustExist(contentItems, "Content items cannot be null");
      mustBeGreater(contentItems.length, 0, illegalArgument("Content items cannot be empty"));

      final var variants = new UniVariant[contentItems.length];
      final var content0 = contentItems[0];
      final var mapping0 = checkedGetMapping(content0.getClass());
      mapping0.mappings.ensureSufficientMappings(contentItems.length);
      variants[0] = new UniVariant(mapping0.handle, null, content0);

      var lastVersion = mapping0.handle.getVersion();
      for (var i = 1; i < contentItems.length; i++) {
        final var content = contentItems[i];
        final var mapping = checkedGetMapping(content.getClass());
        final var _i = i;
        final var _lastVersion = lastVersion;
        lastVersion = mustBeLess(mapping.handle.getVersion(), lastVersion, 
                                 withMessage(() -> 
                                 "Content items should be arranged in decreasing order of version; v" + mapping.handle.getVersion() + 
                                 " at index " + _i + " is later than v" + _lastVersion + " at index " + (_i - 1),
                                 NonDecreasingContentVersionsException::new));
        mustBeEqual(mapping0.handle.getType(), mapping.handle.getType(), 
                    withMessage(() -> 
                    "Mixed content types unsupported; expected: " + mapping0.handle.getType() + " at index " + _i +  
                    ", got: " + mapping.handle.getType(), 
                    MixedContentTypesException::new));
        variants[i] = new UniVariant(mapping.handle, null, content);
      }

      return new MultiVariant(variants);
    }
  }

  private final StandardStrictPreparer strictPreparer = new StandardStrictPreparer();

  public StandardStrictPreparer strict() { return strictPreparer; }
  
  public final class CompactStrictPreparer implements Preparer {
    @Override
    public UniVariant prepare(Object content) {
      mustExist(content, "Content cannot be null");
      final var mapping = checkedGetMapping(content.getClass());
      mapping.mappings.ensureSufficientMappings(1);
      return new UniVariant(mapping.handle, null, content);
    }

    @Override
    public Variant prepare(Object... contentItems) {
      mustExist(contentItems, "Content items cannot be null");
      mustBeGreater(contentItems.length, 0, illegalArgument("Content items cannot be empty"));

      if (contentItems.length == 1) {
        return prepare(contentItems[0]);
      } else {
        return strictPreparer.prepare(contentItems);
      }
    }
  }

  private final CompactStrictPreparer compactStrictPreparer = new CompactStrictPreparer();

  public CompactStrictPreparer compactStrict() { return compactStrictPreparer; }

  public Object map(UniVariant variant) {
    mustExist(variant, "Variant cannot be null");
    final var packed = variant.getPacked();
    final var unpacker = checkedGetUnpacker(packed.getClass());
    final var mapping = mappingForHandle(variant.getHandle());
    if (mapping != null) {
      return unpacker.unpack(Classes.cast(packed), mapping.contentClass);
    } else {
      return null;
    }
  }
  
  public Object map(MultiVariant variant) {
    mustExist(variant, "Variant cannot be null");
    for (var nested : variant.getVariants()) {
      final var mapped = map(nested);
      if (mapped != null) {
        return mapped;
      }
    }
    return null;
  }
  
  public Object map(Variant variant) {
    if (variant instanceof UniVariant) {
      return map((UniVariant) variant);
    } else {
      return map((MultiVariant) variant);
    }
  }

  private VersionMapping mappingForHandle(ContentHandle handle) {
    final var mappings = typeToVersions.get(handle.getType());
    if (mappings != null) {
      final var desiredVersion = handle.getVersion();
      final var numMappings = mappings.list.size();
      for (var mappingIndex = numMappings; --mappingIndex >= 0; ) {
        final var mapping = mappings.list.get(mappingIndex);
        final var mappedVersion = mapping.handle.getVersion();
        if (mappedVersion == desiredVersion) {
          return mapping;
        } else if (mappedVersion < desiredVersion) {
          return null;
        }
      }
      return null;
    } else {
      return null;
    }
  }

  private Unpacker<?> checkedGetUnpacker(Class<? extends PackedForm> packedFormClass) {
    return mustExist(unpackers, packedFormClass, "No unpacker for %s", IllegalStateException::new);
  }

  private VersionMappings getOrCreateVersionMappings(String contentType) {
    return typeToVersions.computeIfAbsent(contentType, __ -> new VersionMappings());
  }
}
