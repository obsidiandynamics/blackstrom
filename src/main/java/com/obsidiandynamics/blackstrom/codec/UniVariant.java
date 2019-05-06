package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.obsidiandynamics.func.*;

/**
 *  An implementation of {@link Variant} that captures just one version of
 *  a content object. The mapping process will either succeed — if the 
 *  {@link ContentMapper} supports the captured version, or will fail with a
 *  {@code null} if the captured version is not supported.
 */
public final class UniVariant implements Variant {
  private final ContentHandle handle;
  private final PackedForm packed;
  private final Object content;
  
  public UniVariant(ContentHandle handle, PackedForm packed, Object content) {
    this.handle = mustExist(handle, "Content handle cannot be null");
    mustBeTrue(packed != null ^ content != null, illegalArgument("Either the packed form or the original content must be specified"));
    mustBeFalse(content instanceof Variant, 
                withMessage(() -> "Cannot nest content of type " + Variant.class.getSimpleName(), 
                            IllegalArgumentException::new));
    this.packed = packed;
    this.content = content;
  }

  public ContentHandle getHandle() {
    return handle;
  }

  public PackedForm getPacked() {
    return packed;
  }

  public <T> T getContent() {
    return Classes.cast(content);
  }
  
  @Override
  public <C> C map(ContentMapper mapper) {
    mustExist(mapper, "Content mapper cannot be null");
    final var unpacker = mapper.checkedGetUnpacker(packed.getClass());
    final var mapping = mapper.mappingForHandle(handle);
    if (mapping != null) {
      return Classes.cast(unpacker.unpack(Classes.cast(packed), mapping.contentClass));
    } else {
      return null;
    }
  }
  
  @Override
  public int hashCode() {
    final var prime = 31;
    var result = 1;
    result = prime * result + Objects.hashCode(handle);
    result = prime * result + Objects.hashCode(packed);
    result = prime * result + Objects.hashCode(content);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof UniVariant) {
      final var that = (UniVariant) obj;
      return Objects.equals(handle, that.handle) && Objects.equals(packed, that.packed) && Objects.equals(content, that.content);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return UniVariant.class.getSimpleName() + " [handle=" + handle + ", packed=" + packed + ", content=" + content + "]";
  }
}
