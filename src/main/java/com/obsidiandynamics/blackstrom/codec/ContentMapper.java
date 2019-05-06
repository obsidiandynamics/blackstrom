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
 *  When a {@link Variant} is <em>captured</em>, a content type and version pair
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
 *  When a {@link Variant} is <em>mapped</em> to a Java class following deserialization
 *  by calling {@link Variant#map(ContentMapper)}, the {@link ContentMapper} is first
 *  consulted to resolve a corresponding mapping for the stored content type and 
 *  version pair, which yields the concrete class type (if a 
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
 *  deserialization impossible without the special handling provided by an 
 *  {@link Unpacker}. <p>
 *  
 *  Once configured with unpackers and version mappings, this class is <em>thread-safe</em>.
 *  
 *  @see Variant
 *  @see UniVariant
 *  @see MultiVariant
 *  @see PackedForm
 *  @see Unpacker
 */
public final class ContentMapper {
  private final Map<Class<? extends PackedForm>, Unpacker<?>> unpackers = new HashMap<>();

  private final Map<String, VersionMappings> typeToVersions = new LinkedHashMap<>();

  private final Map<Class<?>, VersionMapping> classToVersion = new HashMap<>();

  /**
   *  Thrown when registering version mappings in non-increasing order. I.e. it is required
   *  that the successor version mapping must be registered after its predecessor, not before it.
   */
  public static final class IllegalMappingException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    IllegalMappingException(String m) { super(m); }
  }

  /**
   *  Thrown when a precise version mapping could not be resolved for a captured content
   *  object. There must be a one-to-one relationship between captured classes and
   *  version mappings.
   */
  public static final class NoSuchMappingException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    NoSuchMappingException(String m) { super(m); }
  }

  /**
   *  Thrown if an insufficient number of variants were supplied to a <em>strict</em> captor. In other words,
   *  not all supported versions were accounted for by the invoking application. W
   */
  public static final class InsufficientVariantsException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    InsufficientVariantsException(String m) { super(m); }
  }

  /**
   *  Thrown if the variants supplied to a <em>strict</em> captor featured mixed content types.
   *  If the use of heterogenous content types is required (for example, when the content
   *  type must be altered as part of a version upgrade), use a <em>relaxed</em> captor
   *  instead.
   */
  public static final class MixedContentTypesException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    MixedContentTypesException(String m) { super(m); }
  }

  /**
   *  Thrown by a <em>strict</em> captor when the supplied variants are in non-decreasing order
   *  of version number. It is a requirement of a strict captor that the latest version of the
   *  content object is provided first, then the next fallback version, and so on.
   */
  public static final class NonDecreasingContentVersionsException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    NonDecreasingContentVersionsException(String m) { super(m); }
  }

  /**
   *  Holds all {@link VersionMapping}s for a given content type.
   */
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
      mustBeGreaterOrEqual(supplied, list.size(), 
                           withMessage(() -> "Insufficient variants supplied; expected: " + list.size() + ", got: " + supplied, 
                                       InsufficientVariantsException::new));
    }
  }

  /**
   *  Maps a content type and version to a concrete class type.
   */
  static final class VersionMapping {
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

  /**
   *  Provides a string dump of all registered version mappings.
   *  
   *  @return A dump of registered mappings as a {@link String}.
   */
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

  public ContentMapper withVersion(String contentType, int contentVersion, Class<?> contentClass) {
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
  
  public interface Captor {
    Variant capture(Object content);
    
    Variant capture(Object... contentItems);
  }

  public final class StandardRelaxedCaptor implements Captor {
    @Override
    public UniVariant capture(Object content) {
      mustExist(content, "Content cannot be null");
      final var mapping = checkedGetMapping(content.getClass());
      final var handle = mapping.handle;
      return new UniVariant(handle, null, content);
    }

    @Override
    public MultiVariant capture(Object... contentItems) {
      mustExist(contentItems, "Content items cannot be null");
      mustBeGreater(contentItems.length, 0, illegalArgument("Content items cannot be empty"));
      
      final var variants = new UniVariant[contentItems.length];
      for (var i = 0; i < contentItems.length; i++) {
        variants[i] = capture(contentItems[i]);
      }
      return new MultiVariant(variants);
    }
  }

  private final StandardRelaxedCaptor relaxedCaptor = new StandardRelaxedCaptor();

  public StandardRelaxedCaptor relaxed() { return relaxedCaptor; }
  
  public final class CompactRelaxedCaptor implements Captor {
    @Override
    public UniVariant capture(Object content) {
      return relaxedCaptor.capture(content);
    }

    @Override
    public Variant capture(Object... contentItems) {
      mustExist(contentItems, "Content items cannot be null");
      mustBeGreater(contentItems.length, 0, illegalArgument("Content items cannot be empty"));
      if (contentItems.length == 1) {
        return relaxedCaptor.capture(contentItems[0]);
      } else {
        return relaxedCaptor.capture(contentItems);
      }
    }
  }
  
  private final CompactRelaxedCaptor compactRelaxedCaptor = new CompactRelaxedCaptor();
  
  public CompactRelaxedCaptor compactRelaxed() { return compactRelaxedCaptor; }

  public final class StandardStrictCaptor implements Captor {
    @Override
    public MultiVariant capture(Object content) {
      mustExist(content, "Content cannot be null");
      final var mapping = checkedGetMapping(content.getClass());
      mapping.mappings.ensureSufficientMappings(1);
      return new MultiVariant(new UniVariant(mapping.handle, null, content));
    }

    @Override
    public MultiVariant capture(Object... contentItems) {
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

  private final StandardStrictCaptor strictCaptor = new StandardStrictCaptor();

  public StandardStrictCaptor strict() { return strictCaptor; }
  
  public final class CompactStrictCaptor implements Captor {
    @Override
    public UniVariant capture(Object content) {
      mustExist(content, "Content cannot be null");
      final var mapping = checkedGetMapping(content.getClass());
      mapping.mappings.ensureSufficientMappings(1);
      return new UniVariant(mapping.handle, null, content);
    }

    @Override
    public Variant capture(Object... contentItems) {
      mustExist(contentItems, "Content items cannot be null");
      mustBeGreater(contentItems.length, 0, illegalArgument("Content items cannot be empty"));

      if (contentItems.length == 1) {
        return capture(contentItems[0]);
      } else {
        return strictCaptor.capture(contentItems);
      }
    }
  }

  private final CompactStrictCaptor compactStrictCaptor = new CompactStrictCaptor();

  public CompactStrictCaptor compactStrict() { return compactStrictCaptor; }

  public Object map(Variant variant) {
    return mustExist(variant, "Variant cannot be null").map(this);
  }

  Unpacker<?> checkedGetUnpacker(Class<? extends PackedForm> packedFormClass) {
    return mustExist(unpackers, packedFormClass, "No unpacker for %s", IllegalStateException::new);
  }
  
  VersionMapping mappingForHandle(ContentHandle handle) {
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

  private VersionMappings getOrCreateVersionMappings(String contentType) {
    return typeToVersions.computeIfAbsent(contentType, __ -> new VersionMappings());
  }
  
  @Override
  public String toString() {
    return ContentMapper.class.getSimpleName() + " [typeToVersions=" + typeToVersions + ", unpackers=" + unpackers + "]";
  }
}
