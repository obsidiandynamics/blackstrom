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
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", v1, CreateFoo_v1.class)
        .withMapping("test:bar/create", v1, CreateBar_v1.class);
    assertEquals("{test:foo/create=[0 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateFoo_v0, "+
        "1 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateFoo_v1], " + 
        "test:bar/create=[1 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateBar_v1]}", 
        conmap.printSnapshots());
  }

  @Test
  public void testWithMapping_nonIncreasingVersion() {
    final var contentType = "test:foo/create";
    final var conmap = new ContentMapper()
        .withMapping(contentType, 1, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.withMapping(contentType, 1, CreateBar_v1.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v1) is not ahead of the preceding (v1)");

    Assertions.assertThatThrownBy(() -> {
      conmap.withMapping(contentType, 0, CreateBar_v1.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v0) is not ahead of the preceding (v1)");
  }

  @Test
  public void testWithMapping_existingMapping() {
    final var contentType = "test:foo/create";
    final var conmap = new ContentMapper()
        .withMapping(contentType, 0, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.withMapping(contentType, 1, CreateFoo_v0.class);
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
  public void testRelaxedCaptureSingle_normal() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withMapping("test:bar/create", vLegacy, CreateBar_v0.class)
        .withMapping("test:foo/create", v1, CreateFoo_v1.class)
        .withMapping("test:bar/create", v1, CreateBar_v1.class);

    {
      final var p = conmap.relaxed().capture(new CreateFoo_v0());
      assertNull(p.getPacked());
      assertTrue(p.getContent() instanceof CreateFoo_v0);
      assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
    }

    {
      final var p = conmap.relaxed().capture(new CreateFoo_v1());
      assertEquals(new ContentHandle("test:foo/create", 1), p.getHandle());
    }

    {
      final var p = conmap.relaxed().capture(new CreateBar_v1());
      assertEquals(new ContentHandle("test:bar/create", 1), p.getHandle());
    }
  }

  @Test
  public void testRelaxedCaptureMultiple_normal() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withMapping("test:bar/create", vLegacy, CreateBar_v0.class)
        .withMapping("test:foo/create", v1, CreateFoo_v1.class)
        .withMapping("test:bar/create", v1, CreateBar_v1.class);

    final var mp = conmap.relaxed().capture(new CreateFoo_v0(), new CreateBar_v1());
    assertEquals(2, mp.getVariants().length);

    final var p0 = mp.getVariants()[0];
    assertTrue(p0.getContent() instanceof CreateFoo_v0);
    assertEquals(new ContentHandle("test:foo/create", 0), p0.getHandle());

    final var p1 = mp.getVariants()[1];
    assertTrue(p1.getContent() instanceof CreateBar_v1);
    assertEquals(new ContentHandle("test:bar/create", 1), p1.getHandle());
  }

  @Test
  public void testRelaxedCaptureSingle_missingMapping() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.relaxed().capture(new CreateFoo_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateFoo_v1.class);
  }

  @Test
  public void testCompactRelaxedCaptureSingle_normal() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withMapping("test:bar/create", vLegacy, CreateBar_v0.class)
        .withMapping("test:foo/create", v1, CreateFoo_v1.class)
        .withMapping("test:bar/create", v1, CreateBar_v1.class);

    {
      final var p = conmap.compactRelaxed().capture(new CreateFoo_v0());
      assertNull(p.getPacked());
      assertTrue(p.getContent() instanceof CreateFoo_v0);
      assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
    }

    {
      final var p = conmap.relaxed().capture(new CreateFoo_v1());
      assertEquals(new ContentHandle("test:foo/create", 1), p.getHandle());
    }

    {
      final var p = conmap.relaxed().capture(new CreateBar_v1());
      assertEquals(new ContentHandle("test:bar/create", 1), p.getHandle());
    }
  }

  @Test
  public void testCompactRelaxedCaptureMultiple_normalWithOne() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withMapping("test:bar/create", vLegacy, CreateBar_v0.class)
        .withMapping("test:foo/create", v1, CreateFoo_v1.class)
        .withMapping("test:bar/create", v1, CreateBar_v1.class);

    final var p = mustBeSubtype(conmap.compactRelaxed().capture(new Object[] { new CreateFoo_v0() }), UniVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
  }

  @Test
  public void testCompactRelaxedCaptureMultiple_normalWithMany() {
    final var vLegacy = 0;
    final var v1 = 1;
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", vLegacy, CreateFoo_v0.class)
        .withMapping("test:bar/create", vLegacy, CreateBar_v0.class)
        .withMapping("test:foo/create", v1, CreateFoo_v1.class)
        .withMapping("test:bar/create", v1, CreateBar_v1.class);

    final var mp = mustBeSubtype(conmap.compactRelaxed().capture(new CreateFoo_v0(), new CreateBar_v1()), MultiVariant.class, AssertionError::new);
    assertEquals(2, mp.getVariants().length);

    final var p0 = mp.getVariants()[0];
    assertTrue(p0.getContent() instanceof CreateFoo_v0);
    assertEquals(new ContentHandle("test:foo/create", 0), p0.getHandle());

    final var p1 = mp.getVariants()[1];
    assertTrue(p1.getContent() instanceof CreateBar_v1);
    assertEquals(new ContentHandle("test:bar/create", 1), p1.getHandle());
  }

  @Test
  public void testStrictCaptureSingle_normal() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    final var mp = conmap.strict().capture(new CreateBar_v0());
    assertEquals(1, mp.getVariants().length);
    assertEquals(new ContentHandle("test:bar/create", 0), mp.getVariants()[0].getHandle());
  }

  @Test
  public void testStrictCaptureSingle_missingMapping() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().capture(new CreateBar_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateBar_v1.class);
  }

  @Test
  public void testStrictCaptureSingle_insufficientVariants() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().capture(new CreateFoo_v1());
    }).isInstanceOf(InsufficientVariantsException.class).hasMessage("Insufficient variants supplied; expected: 2, got: 1");
  }

  @Test
  public void testStrictCaptureMultiple_normalWithOne() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    final var mp = conmap.strict().capture(new Object[] { new CreateBar_v0() });
    assertEquals(1, mp.getVariants().length);
    assertEquals(new ContentHandle("test:bar/create", 0), mp.getVariants()[0].getHandle());
  }

  @Test
  public void testStrictCaptureMultiple_normalWithMany() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    final var mp = conmap.strict().capture(new Object[] { new CreateFoo_v1(), new CreateFoo_v0() });
    assertEquals(2, mp.getVariants().length);
    assertEquals(new ContentHandle("test:foo/create", 1), mp.getVariants()[0].getHandle());
    assertEquals(new ContentHandle("test:foo/create", 0), mp.getVariants()[1].getHandle());
  }

  @Test
  public void testStrictCaptureMultiple_nonDecreasingVersion() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().capture(new Object[] { new CreateFoo_v0(), new CreateFoo_v1() });
    })
    .isInstanceOf(NonDecreasingContentVersionsException.class)
    .hasMessage("Content items should be arranged in decreasing order of version; v1 at index 1 is later than v0 at index 0");
  }

  @Test
  public void testStrictCaptureMultiple_mixedContentTypes() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.strict().capture(new Object[] { new CreateFoo_v1(), new CreateBar_v0() });
    })
    .isInstanceOf(MixedContentTypesException.class)
    .hasMessage("Mixed content types unsupported; expected: test:foo/create at index 1, got: test:bar/create");
  }

  @Test
  public void testCompactStrictCaptureSingle_normal() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    final var p = mustBeSubtype(conmap.compactStrict().capture(new CreateBar_v0()), UniVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:bar/create", 0), p.getHandle());
  }

  @Test
  public void testCompactStrictCaptureSingle_missingMapping() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().capture(new CreateBar_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateBar_v1.class);
  }

  @Test
  public void testCompactStrictCaptureSingle_insufficientVariants() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().capture(new CreateFoo_v1());
    }).isInstanceOf(InsufficientVariantsException.class).hasMessage("Insufficient variants supplied; expected: 2, got: 1");
  }

  @Test
  public void testCompactStrictCaptureMultiple_normalWithOne() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    final var p = mustBeSubtype(conmap.compactStrict().capture(new Object[] { new CreateBar_v0() }), UniVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:bar/create", 0), p.getHandle());
  }

  @Test
  public void testCompactStrictCaptureMultiple_normalWithMany() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    final var mp = mustBeSubtype(conmap.compactStrict().capture(new Object[] { new CreateFoo_v1(), new CreateFoo_v0() }), MultiVariant.class, AssertionError::new);
    assertEquals(2, mp.getVariants().length);
    assertEquals(new ContentHandle("test:foo/create", 1), mp.getVariants()[0].getHandle());
    assertEquals(new ContentHandle("test:foo/create", 0), mp.getVariants()[1].getHandle());
  }

  @Test
  public void testCompactStrictCaptureMultiple_nonDecreasingVersion() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().capture(new Object[] { new CreateFoo_v0(), new CreateFoo_v1() });
    })
    .isInstanceOf(NonDecreasingContentVersionsException.class)
    .hasMessage("Content items should be arranged in decreasing order of version; v1 at index 1 is later than v0 at index 0");
  }

  @Test
  public void testCompactStrictCaptureMultiple_mixedContentTypes() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      conmap.compactStrict().capture(new Object[] { new CreateFoo_v1(), new CreateBar_v0() });
    })
    .isInstanceOf(MixedContentTypesException.class)
    .hasMessage("Mixed content types unsupported; expected: test:foo/create at index 1, got: test:bar/create");
  }

  private static UniVariant emulatePacked(String contentType, int contentVersion, Object content) {
    return new UniVariant(new ContentHandle(contentType, contentVersion), new IdentityPackedForm(content), null);
  }

  @Test
  public void testMap_uni_matching() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 1, CreateBar_v1.class);

    {
      final var v0 = new CreateFoo_v0();
      final var p = emulatePacked("test:foo/create", 0, v0);
      assertTrue(p.getPacked() instanceof IdentityPackedForm);
      assertNull(p.getContent());
      assertSame(v0, conmap.map(p));
      assertSame(v0, conmap.map((Variant) p));
    }
    {
      final var v1 = new CreateFoo_v1();
      final var p = emulatePacked("test:foo/create", 1, v1);
      assertSame(v1, conmap.map(p));
      assertSame(v1, conmap.map((Variant) p));
    }
    {
      final var v0 = new CreateBar_v0();
      final var p = emulatePacked("test:bar/create", 0, v0);
      assertSame(v0, conmap.map(p));
      assertSame(v0, conmap.map((Variant) p));
    }
  }

  @Test
  public void testMap_uni_noMatchingContentType() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class);

    final var v0 = new CreateBar_v0();
    final var p = emulatePacked("test:bar/create", 0, v0);
    assertNull(conmap.map(p));
    assertNull(conmap.map((Variant) p));
  }

  @Test
  public void testMap_uni_noMatchingLegacyVersion() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withMapping("test:foo/create", 0, CreateFoo_v0.class);

    final var v1 = new CreateFoo_v1();
    final var p = emulatePacked("test:foo/create", 1, v1);
    assertNull(conmap.map(p));
    assertNull(conmap.map((Variant) p));
  }

  @Test
  public void testMap_uni_obsoletedVersion() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withMapping("test:foo/create", 1, CreateFoo_v1.class);

    final var v0 = new CreateFoo_v0();
    final var p = emulatePacked("test:foo/create", 0, v0);
    assertNull(conmap.map(p));
    assertNull(conmap.map((Variant) p));
  }

  @Test
  public void testMap_uni_noUnpacker() {
    final var conmap = new ContentMapper()
        .withMapping("test:foo/create", 1, CreateFoo_v1.class);

    final var v1 = new CreateFoo_v1();
    final var p = emulatePacked("test:foo/create", 1, v1);
    Assertions.assertThatThrownBy(() -> {
      conmap.map(p);
    }).isInstanceOf(IllegalStateException.class).hasMessage("No unpacker for " + IdentityPackedForm.class);
  }

  @Test
  public void testMap_multi_matchingFirst() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class)
        .withMapping("test:foo/create", 1, CreateFoo_v1.class)
        .withMapping("test:bar/create", 1, CreateBar_v1.class);

    final var v1 = new CreateFoo_v1();
    final var v0 = new CreateFoo_v0();
    final var mp = new MultiVariant(emulatePacked("test:foo/create", 1, v1), emulatePacked("test:foo/create", 0, v0));
    assertSame(v1, conmap.map(mp));
    assertSame(v1, conmap.map((Variant) mp));
  }

  @Test
  public void testMap_multi_matchingFallback() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withMapping("test:foo/create", 0, CreateFoo_v0.class)
        .withMapping("test:bar/create", 0, CreateBar_v0.class);

    final var v1 = new CreateFoo_v1();
    final var v0 = new CreateFoo_v0();
    final var mp = new MultiVariant(emulatePacked("test:foo/create", 1, v1), emulatePacked("test:foo/create", 0, v0));
    assertSame(v0, conmap.map(mp));
    assertSame(v0, conmap.map((Variant) mp));
  }

  @Test
  public void testMap_multi_obsoleted() {
    final var conmap = new ContentMapper()
        .withUnpacker(new IdentityUnpacker());;

        final var v1 = new CreateFoo_v1();
        final var v0 = new CreateFoo_v0();
        final var mp = new MultiVariant(emulatePacked("test:foo/create", 1, v1), emulatePacked("test:foo/create", 0, v0));
        assertNull(conmap.map(mp));
        assertNull(conmap.map((Variant) mp));
  }
}
