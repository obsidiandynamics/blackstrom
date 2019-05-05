package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;
import static org.junit.Assert.*;

import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.blackstrom.codec.ContentMapper.*;

public final class ContentMapperTest {
  private static final class CreateFoo_v0 {}

  private static final class CreateBar_v0 {}

  private static final class CreateFoo_v1 {}

  private static final class CreateBar_v1 {}

  @Test
  public void testPrintSnapshots_empty() {
    final var conmap = new ContentMapper();
    assertEquals("{}", conmap.printSnapshots());
  }

  @Test
  public void testPrintSnapshots_nonEmpty() {
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", v1, CreateBar_v1.class);
    assertEquals("{test:foo/create=[0 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateFoo_v0, "+
        "1 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateFoo_v1], " + 
        "test:bar/create=[1 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateBar_v1]}", 
        conmap.printSnapshots());
  }

  @Test
  public void testWithSnapshot_nonIncreasingVersion() {
    final var contentType = "test:foo/create";
    final var conmap = new ContentMapper()
        .withSnapshot(contentType, 1, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.withSnapshot(contentType, 1, CreateBar_v1.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v1) is not ahead of the preceding (v1)");

    Assertions.assertThatThrownBy(() -> {
      conmap.withSnapshot(contentType, 0, CreateBar_v1.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v0) is not ahead of the preceding (v1)");
  }

  @Test
  public void testWithSnapshot_existingMapping() {
    final var contentType = "test:foo/create";
    final var conmap = new ContentMapper()
        .withSnapshot(contentType, 0, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.withSnapshot(contentType, 1, CreateFoo_v0.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("A mapping already exists for content " + CreateFoo_v0.class);
  }

  @Test
  public void testWithUnpacker_duplicate() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker());

    Assertions.assertThatThrownBy(() -> {
      conmap.withUnpacker(new IdentityUnpacker());
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Duplicate unpacker for class " + IdentityPackedForm.class.getName());
  }

  @Test
  public void testRelaxedPrepareSingle_normal() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", v1, CreateBar_v1.class);

    {
      final var p = conmap.relaxed().prepare(new CreateFoo_v0());
      assertNull(p.getPacked());
      assertTrue(p.getContent() instanceof CreateFoo_v0);
      assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
    }

    {
      final var p = conmap.relaxed().prepare(new CreateFoo_v1());
      assertEquals(new ContentHandle("test:foo/create", 1), p.getHandle());
    }

    {
      final var p = conmap.relaxed().prepare(new CreateBar_v1());
      assertEquals(new ContentHandle("test:bar/create", 1), p.getHandle());
    }
  }

  @Test
  public void testRelaxedPrepareMultiple_normal() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", v1, CreateBar_v1.class);

    final var mp = conmap.relaxed().prepare(new CreateFoo_v0(), new CreateBar_v1());
    assertEquals(2, mp.getVariants().length);
    
    final var p0 = mp.getVariants()[0];
    assertTrue(p0.getContent() instanceof CreateFoo_v0);
    assertEquals(new ContentHandle("test:foo/create", 0), p0.getHandle());
    
    final var p1 = mp.getVariants()[1];
    assertTrue(p1.getContent() instanceof CreateBar_v1);
    assertEquals(new ContentHandle("test:bar/create", 1), p1.getHandle());
  }

  @Test
  public void testRelaxedPrepareSingle_missingMapping() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.relaxed().prepare(new CreateFoo_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateFoo_v1.class);
  }

  @Test
  public void testCompactRelaxedPrepareSingle_normal() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", v1, CreateBar_v1.class);

    {
      final var p = conmap.compactRelaxed().prepare(new CreateFoo_v0());
      assertNull(p.getPacked());
      assertTrue(p.getContent() instanceof CreateFoo_v0);
      assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
    }

    {
      final var p = conmap.relaxed().prepare(new CreateFoo_v1());
      assertEquals(new ContentHandle("test:foo/create", 1), p.getHandle());
    }

    {
      final var p = conmap.relaxed().prepare(new CreateBar_v1());
      assertEquals(new ContentHandle("test:bar/create", 1), p.getHandle());
    }
  }

  @Test
  public void testCompactRelaxedPrepareMultiple_normalWithOne() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", v1, CreateBar_v1.class);

    final var p = mustBeSubtype(conmap.compactRelaxed().prepare(new Object[] { new CreateFoo_v0() }), UniVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
  }

  @Test
  public void testCompactRelaxedPrepareMultiple_normalWithMany() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", v1, CreateBar_v1.class);

    final var mp = mustBeSubtype(conmap.compactRelaxed().prepare(new CreateFoo_v0(), new CreateBar_v1()), MultiVariant.class, AssertionError::new);
    assertEquals(2, mp.getVariants().length);
    
    final var p0 = mp.getVariants()[0];
    assertTrue(p0.getContent() instanceof CreateFoo_v0);
    assertEquals(new ContentHandle("test:foo/create", 0), p0.getHandle());
    
    final var p1 = mp.getVariants()[1];
    assertTrue(p1.getContent() instanceof CreateBar_v1);
    assertEquals(new ContentHandle("test:bar/create", 1), p1.getHandle());
  }

  @Test
  public void testStrictPrepareSingle_normal() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    final var mp = conmap.strict().prepare(new CreateBar_v0());
    assertEquals(1, mp.getVariants().length);
    assertEquals(new ContentHandle("test:bar/create", 0), mp.getVariants()[0].getHandle());
  }

  @Test
  public void testStrictPrepareSingle_missingMapping() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().prepare(new CreateBar_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateBar_v1.class);
  }

  @Test
  public void testStrictPrepareSingle_insufficientVariants() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().prepare(new CreateFoo_v1());
    }).isInstanceOf(InsufficientVariantsException.class).hasMessage("Insufficient variants supplied; expected: 2, got: 1");
  }

  @Test
  public void testStrictPrepareMultiple_normalWithOne() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    final var mp = conmap.strict().prepare(new Object[] { new CreateBar_v0() });
    assertEquals(1, mp.getVariants().length);
    assertEquals(new ContentHandle("test:bar/create", 0), mp.getVariants()[0].getHandle());
  }

  @Test
  public void testStrictPrepareMultiple_normalWithMany() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    final var mp = conmap.strict().prepare(new Object[] { new CreateFoo_v1(), new CreateFoo_v0() });
    assertEquals(2, mp.getVariants().length);
    assertEquals(new ContentHandle("test:foo/create", 1), mp.getVariants()[0].getHandle());
    assertEquals(new ContentHandle("test:foo/create", 0), mp.getVariants()[1].getHandle());
  }

  @Test
  public void testStrictPrepareMultiple_nonDecreasingVersion() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().prepare(new Object[] { new CreateFoo_v0(), new CreateFoo_v1() });
    })
    .isInstanceOf(NonDecreasingContentVersionsException.class)
    .hasMessage("Content items should be arranged in decreasing order of version; v1 at index 1 is later than v0 at index 0");
  }

  @Test
  public void testStrictPrepareMultiple_mixedContentTypes() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().prepare(new Object[] { new CreateFoo_v1(), new CreateBar_v0() });
    })
    .isInstanceOf(MixedContentTypesException.class)
    .hasMessage("Mixed content types unsupported; expected: test:foo/create at index 1, got: test:bar/create");
  }

  @Test
  public void testCompactStrictPrepareSingle_normal() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    final var p = mustBeSubtype(conmap.compactStrict().prepare(new CreateBar_v0()), UniVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:bar/create", 0), p.getHandle());
  }

  @Test
  public void testCompactStrictPrepareSingle_missingMapping() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().prepare(new CreateBar_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateBar_v1.class);
  }

  @Test
  public void testCompactStrictPrepareSingle_insufficientVariants() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().prepare(new CreateFoo_v1());
    }).isInstanceOf(InsufficientVariantsException.class).hasMessage("Insufficient variants supplied; expected: 2, got: 1");
  }

  @Test
  public void testCompactStrictPrepareMultiple_normalWithOne() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    final var p = mustBeSubtype(conmap.compactStrict().prepare(new Object[] { new CreateBar_v0() }), UniVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:bar/create", 0), p.getHandle());
  }

  @Test
  public void testCompactStrictPrepareMultiple_normalWithMany() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    final var mp = mustBeSubtype(conmap.compactStrict().prepare(new Object[] { new CreateFoo_v1(), new CreateFoo_v0() }), MultiVariant.class, AssertionError::new);
    assertEquals(2, mp.getVariants().length);
    assertEquals(new ContentHandle("test:foo/create", 1), mp.getVariants()[0].getHandle());
    assertEquals(new ContentHandle("test:foo/create", 0), mp.getVariants()[1].getHandle());
  }

  @Test
  public void testCompactStrictPrepareMultiple_nonDecreasingVersion() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().prepare(new Object[] { new CreateFoo_v0(), new CreateFoo_v1() });
    })
    .isInstanceOf(NonDecreasingContentVersionsException.class)
    .hasMessage("Content items should be arranged in decreasing order of version; v1 at index 1 is later than v0 at index 0");
  }

  @Test
  public void testCompactStrictPrepareMultiple_mixedContentTypes() {
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", 0, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", 1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().prepare(new Object[] { new CreateFoo_v1(), new CreateBar_v0() });
    })
    .isInstanceOf(MixedContentTypesException.class)
    .hasMessage("Mixed content types unsupported; expected: test:foo/create at index 1, got: test:bar/create");
  }

  private static UniVariant emulatePacked(String contentType, int contentVersion, Object content) {
    return new UniVariant(new ContentHandle(contentType, contentVersion), new IdentityPackedForm(content), null);
  }

  @Test
  public void testRelaxedMap_normal() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:bar/create", vLegacy, CreateBar_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class)
        .withSnapshot("test:bar/create", v1, CreateBar_v1.class);

    {
      final var content = new CreateFoo_v0();
      final var p = emulatePacked("test:foo/create", vLegacy, content);
      assertTrue(p.getPacked() instanceof IdentityPackedForm);
      assertNull(p.getContent());
      assertSame(content, conmap.map(p));
    }
    {
      final var content = new CreateFoo_v1();
      final var p = emulatePacked("test:foo/create", v1, content);
      assertSame(content, conmap.map(p));
    }
    {
      final var content = new CreateBar_v0();
      final var p = emulatePacked("test:bar/create", vLegacy, content);
      assertSame(content, conmap.map(p));
    }
  }

  @Test
  public void testRelaxedMap_noMatchingContentType() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class);

    final var content = new CreateBar_v0();
    final var p = emulatePacked("test:bar/create", vLegacy, content);
    assertNull(conmap.map(p));
  }

  @Test
  public void testRelaxedMap_noMatchingLegacyVersion() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", vLegacy, CreateFoo_v0.class);

    final var content = new CreateFoo_v1();
    final var p = emulatePacked("test:foo/create", v1, content);
    assertNull(conmap.map(p));
  }

  @Test
  public void testRelaxedMap_obsoletedVersion() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class);

    final var content = new CreateFoo_v0();
    final var p = emulatePacked("test:foo/create", vLegacy, content);
    assertNull(conmap.map(p));
  }

  @Test
  public void testRelaxedMap_noUnpacker() {
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withSnapshot("test:foo/create", v1, CreateFoo_v1.class);

    final var content = new CreateFoo_v1();
    final var p = emulatePacked("test:foo/create", v1, content);
    Assertions.assertThatThrownBy(() -> {
      conmap.map(p);
    }).isInstanceOf(IllegalStateException.class).hasMessage("No unpacker for " + IdentityPackedForm.class);
  }
}
