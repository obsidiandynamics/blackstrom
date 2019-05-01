package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

/**
 *  Composition of content type and content version attributes.
 */
public final class ContentHandle {
  private final String type;
  
  private final int version;

  public ContentHandle(String type, int version) {
    this.type = validateContentType(type);
    this.version = validateContentVersion(version);
  }

  public String getType() {
    return type;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public int hashCode() {
    final var prime = 31;
    var result = 1;
    result = prime * result + Objects.hashCode(type);
    result = prime * result + version;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof ContentHandle) {
      final var that = (ContentHandle) obj;
      return Objects.equals(type, that.type) && version == that.version;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return ContentHandle.class.getSimpleName() + " [type=" + type + ", version=" + version + "]";
  }
  
  static String validateContentType(String contentType) {
    mustExist(contentType, "Content type cannot be null");
    mustBeGreater(contentType.length(), 0, illegalArgument("Content type cannot be empty"));
    return contentType;
  }
  
  static int validateContentVersion(int contentVersion) {
    return mustBeGreaterOrEqual(contentVersion, 0, illegalArgument("Content version must not be negative"));
  }
}
