package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.obsidiandynamics.func.*;

/**
 *  Registry of {@link Unpacker} implementations and version mapping snapshots.
 */
public final class ContentVersions {
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
  }
  
  private static final class VersionMapping {
    final ContentHandle handle;
    
    final Class<?> contentClass;

    VersionMapping(ContentHandle handle, Class<?> contentClass) {
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
  
  public ContentVersions withUnpacker(Unpacker<?> unpacker) {
    mustExist(unpacker, "Unpacker cannot be null");
    final var packedClass = unpacker.getPackedType();
    mustBeFalse(unpackers.containsKey(packedClass), 
                withMessage(() -> "Duplicate unpacker for class " + packedClass.getName(), IllegalArgumentException::new));
    unpackers.put(Classes.cast(packedClass), unpacker);
    return this;
  }
  
  public ContentVersions withSnapshot(String contentType, int contentVersion, Class<?> contentClass) {
    ContentHandle.validateContentType(contentType);
    ContentHandle.validateContentVersion(contentVersion);
    mustExist(contentClass, "Content class cannot be null");
    final var mappings = getOrCreateVersionMappings(contentType);
    final var mapping = new VersionMapping(new ContentHandle(contentType, contentVersion), contentClass);
    mappings.add(mapping);
    classToVersion.put(contentClass, mapping);
    return this;
  }
  
  public Versionable pack(Object content) {
    mustExist(content, "Content must not be null");
    final var mapping = mustExist(classToVersion, content.getClass(), "No mapping for %s", NoSuchMappingException::new);
    final var handle = mapping.handle;
    return pack(content, handle);
  }
  
  public Versionable pack(Object content, ContentHandle handle) {
    mustExist(content, "Content must not be null");
    return new Versionable(handle, null, content);
  }
  
  public Object unpack(Versionable versionable) {
    mustExist(versionable, "Versionable content cannot be null");
    final var packed = versionable.getPacked();
    final var unpacker = checkedGetUnpacker(packed.getClass());
    final var mapping = resolveMapperForHandle(versionable.getHandle());
    if (mapping != null) {
      return unpacker.unpack(Classes.cast(packed), mapping.contentClass);
    } else {
      return null;
    }
  }
  
  private VersionMapping resolveMapperForHandle(ContentHandle handle) {
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
