package com.obsidiandynamics.blackstrom.codec;

import static org.junit.Assert.*;

import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.blackstrom.codec.ContentVersions.*;

public final class ContentVersionsTest {
  private static final class CreateFoo_v0 {}
  
  private static final class CreateBar_v0 {}
  
  private static final class CreateFoo_vLatest {}
  
  private static final class CreateBar_vLatest {}
  
  @Test
  public void testPrintSnapshots_empty() {
    final var vers = new ContentVersions();
    assertEquals("{}", vers.printSnapshots());
  }
  
  @Test
  public void testPrintSnapshots_nonEmpty() {
    final var vLatest = 1;
    final var vers = new ContentVersions()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", vLatest, CreateFoo_vLatest.class)
        .withSnapshot("test:bar/create", vLatest, CreateBar_vLatest.class);
    assertEquals("{test:foo/create=[0 -> com.obsidiandynamics.blackstrom.codec.ContentVersionsTest$CreateFoo_v0, "+
                 "1 -> com.obsidiandynamics.blackstrom.codec.ContentVersionsTest$CreateFoo_vLatest], " + 
                 "test:bar/create=[1 -> com.obsidiandynamics.blackstrom.codec.ContentVersionsTest$CreateBar_vLatest]}", 
                 vers.printSnapshots());
  }
  
  @Test
  public void testWithSnapshot_nonIncreasingVersion() {
    final var contentType = "test:foo/create";
    final var vers = new ContentVersions()
        .withSnapshot(contentType, 1, CreateFoo_v0.class);
    
    Assertions.assertThatThrownBy(() -> {
      vers.withSnapshot(contentType, 1, CreateBar_vLatest.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v1) is not ahead of the preceding (v1)");
    
    Assertions.assertThatThrownBy(() -> {
      vers.withSnapshot(contentType, 0, CreateBar_vLatest.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v0) is not ahead of the preceding (v1)");
  }

  @Test
  public void testWithSnapshot_existingMapping() {
    final var contentType = "test:foo/create";
    final var vers = new ContentVersions()
        .withSnapshot(contentType, 0, CreateFoo_v0.class);
    
    Assertions.assertThatThrownBy(() -> {
      vers.withSnapshot(contentType, 1, CreateFoo_v0.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("A mapping already exists for content " + CreateFoo_v0.class);
  }
  
  @Test
  public void testWithUnpacker_duplicate() {
    final var vers = new ContentVersions()
        .withUnpacker(new IdentityUnpacker());
    
    Assertions.assertThatThrownBy(() -> {
      vers.withUnpacker(new IdentityUnpacker());
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Duplicate unpacker for class " + IdentityPackedForm.class.getName());
  }
  
  @Test
  public void testPack_normal() {
    final var vLegacy = 0;
    final var vLatest = 1;
    final var vers = new ContentVersions()
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", vLatest, CreateFoo_vLatest.class)
        .withSnapshot("test:bar/create", vLatest, CreateBar_vLatest.class);
    
    {
      final var packed = vers.pack(new CreateFoo_v0());
      assertNull(packed.getPacked());
      assertTrue(packed.getContent() instanceof CreateFoo_v0);
      assertEquals(packed.getHandle(), new ContentHandle("test:foo/create", 0));
    }
    
    {
      final var packed = vers.pack(new CreateFoo_vLatest());
      assertEquals(packed.getHandle(), new ContentHandle("test:foo/create", 1));
    }
    
    {
      final var packed = vers.pack(new CreateBar_vLatest());
      assertEquals(packed.getHandle(), new ContentHandle("test:bar/create", 1));
    }
  }
  
  @Test
  public void testPack_missingMapping() {
    final var vers = new ContentVersions()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class);
    
    Assertions.assertThatThrownBy(() -> {
      vers.pack(new CreateFoo_vLatest());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateFoo_vLatest.class);
  }
  
  private static Versionable emulatePacked(String contentType, int contentVersion, Object content) {
    return new Versionable(new ContentHandle(contentType, contentVersion), new IdentityPackedForm(content), null);
  }
  
  @Test
  public void testUnpack_normal() {
    final var vLegacy = 0;
    final var vLatest = 1;
    final var vers = new ContentVersions()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", vLatest, CreateFoo_vLatest.class)
        .withSnapshot("test:bar/create", vLatest, CreateBar_vLatest.class);
    
    {
      final var content = new CreateFoo_v0();
      final var packed = emulatePacked("test:foo/create", vLegacy, content);
      assertTrue(packed.getPacked() instanceof IdentityPackedForm);
      assertNull(packed.getContent());
      assertSame(content, vers.unpack(packed));
    }
    {
      final var content = new CreateFoo_vLatest();
      final var packed = emulatePacked("test:foo/create", vLatest, content);
      assertSame(content, vers.unpack(packed));
    }
    {
      final var content = new CreateBar_v0();
      final var packed = emulatePacked("test:bar/create", vLegacy, content);
      assertSame(content, vers.unpack(packed));
    }
  }
  
  @Test
  public void testUnpack_noMatchingContentType() {
    final var vLegacy = 0;
    final var vLatest = 1;
    final var vers = new ContentVersions()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", vLatest, CreateFoo_vLatest.class);
    
    final var content = new CreateBar_v0();
    final var packed = emulatePacked("test:bar/create", vLegacy, content);
    assertNull(vers.unpack(packed));
  }
  
  @Test
  public void testUnpack_noMatchingLegacyVersion() {
    final var vLegacy = 0;
    final var vLatest = 1;
    final var vers = new ContentVersions()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class);
    
    final var content = new CreateFoo_vLatest();
    final var packed = emulatePacked("test:foo/create", vLatest, content);
    assertNull(vers.unpack(packed));
  }
  
  @Test
  public void testUnpack_obsoletedVersion() {
    final var vLegacy = 0;
    final var vLatest = 1;
    final var vers = new ContentVersions()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", vLatest, CreateFoo_vLatest.class);
    
    final var content = new CreateFoo_v0();
    final var packed = emulatePacked("test:foo/create", vLegacy, content);
    assertNull(vers.unpack(packed));
  }
  
  @Test
  public void testUnpack_noUnpacker() {
    final var vLatest = 1;
    final var vers = new ContentVersions()
        .withSnapshot("test:foo/create", vLatest, CreateFoo_vLatest.class);
    
    final var content = new CreateFoo_vLatest();
    final var packed = emulatePacked("test:foo/create", vLatest, content);
    Assertions.assertThatThrownBy(() -> {
      vers.unpack(packed);
    }).isInstanceOf(IllegalStateException.class).hasMessage("No unpacker for " + IdentityPackedForm.class);
  }
}
