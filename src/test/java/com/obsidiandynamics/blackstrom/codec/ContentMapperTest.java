package com.obsidiandynamics.blackstrom.codec;

import static com.obsidiandynamics.func.Functions.*;
import static org.junit.Assert.*;

import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.blackstrom.codec.ContentMapper.*;

import pl.pojo.tester.internal.assertion.tostring.*;

public final class ContentMapperTest {
  private static final class CreateFoo_v0 {}

  private static final class CreateBar_v0 {}

  private static final class CreateFoo_v1 {}

  private static final class CreateBar_v1 {}

  @Test
  public void testPrintVersions_default() {
    final var mapper = new ContentMapper();
    assertEquals("{std:nil=[1 -> com.obsidiandynamics.blackstrom.codec.Nil]}", mapper.printVersions());
  }

  @Test
  public void testPrintVersions_withCustom() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);
    assertEquals("{std:nil=[1 -> com.obsidiandynamics.blackstrom.codec.Nil], " + 
        "test:foo/create=[0 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateFoo_v0, "+
        "1 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateFoo_v1], " + 
        "test:bar/create=[1 -> com.obsidiandynamics.blackstrom.codec.ContentMapperTest$CreateBar_v1]}", 
        mapper.printVersions());
  }

  @Test
  public void testToString() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withVersion("test:foo/create", 0, CreateFoo_v0.class);

    new ToStringAssertions(mapper)
    .contains("typeToVersions", mapper.printVersions());

    new ToStringAssertions(mapper)
    .contains("unpackers", "{class com.obsidiandynamics.blackstrom.codec.IdentityPackedForm=IdentityUnpacker}");
  }

  @Test
  public void testWithVersion_nonIncreasingVersion() {
    final var contentType = "test:foo/create";
    final var mapper = new ContentMapper()
        .withVersion(contentType, 1, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.withVersion(contentType, 1, CreateBar_v1.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v1) is not ahead of the preceding (v1)");

    Assertions.assertThatThrownBy(() -> {
      mapper.withVersion(contentType, 0, CreateBar_v1.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("Next mapping (v0) is not ahead of the preceding (v1)");
  }

  @Test
  public void testWithVersion_existingMapping() {
    final var contentType = "test:foo/create";
    final var mapper = new ContentMapper()
        .withVersion(contentType, 0, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.withVersion(contentType, 1, CreateFoo_v0.class);
    }).isInstanceOf(IllegalMappingException.class).hasMessage("A mapping already exists for content " + CreateFoo_v0.class);
  }

  @Test
  public void testWithUnpacker_duplicate() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker());

    Assertions.assertThatThrownBy(() -> {
      mapper.withUnpacker(new IdentityUnpacker());
    }).isInstanceOf(IllegalArgumentException.class).hasMessage("Duplicate unpacker for class " + IdentityPackedForm.class.getName());
  }

  @Test
  public void testCaptureAndMapNil() {
    final var mapper = new ContentMapper();
    final var p = mapper.capture(Nil.getInstance());
    assertEquals(Nil.capture(), p);
    assertSame(Nil.getInstance(), p.map(mapper));
  }

  @Test
  public void testRelaxedCaptureUni_normal() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);

    {
      final var p = mapper.relaxed().capture(new CreateFoo_v0());
      assertNull(p.getVariants()[0].getPacked());
      assertTrue(p.getVariants()[0].getContent() instanceof CreateFoo_v0);
      assertEquals(new ContentHandle("test:foo/create", 0), p.getVariants()[0].getHandle());
    }

    {
      final var p = mapper.relaxed().capture(new CreateFoo_v1());
      assertEquals(new ContentHandle("test:foo/create", 1), p.getVariants()[0].getHandle());
    }

    {
      final var p = mapper.relaxed().capture(new CreateBar_v1());
      assertEquals(new ContentHandle("test:bar/create", 1), p.getVariants()[0].getHandle());
    }
  }

  @Test
  public void testRelaxedCapturePoly_normal() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);

    final var mp = mapper.relaxed().capture(new CreateFoo_v0(), new CreateBar_v1());
    assertEquals(2, mp.getVariants().length);

    final var p0 = mp.getVariants()[0];
    assertTrue(p0.getContent() instanceof CreateFoo_v0);
    assertEquals(new ContentHandle("test:foo/create", 0), p0.getHandle());

    final var p1 = mp.getVariants()[1];
    assertTrue(p1.getContent() instanceof CreateBar_v1);
    assertEquals(new ContentHandle("test:bar/create", 1), p1.getHandle());
  }

  @Test
  public void testRelaxedCaptureUni_missingMapping() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.relaxed().capture(new CreateFoo_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateFoo_v1.class);
  }

  @Test
  public void testCompactRelaxedCaptureUni_normal() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);

    {
      final var p = mapper.compactRelaxed().capture(new CreateFoo_v0());
      assertNull(p.getPacked());
      assertTrue(p.getContent() instanceof CreateFoo_v0);
      assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
    }

    {
      final var p = mapper.relaxed().capture(new CreateFoo_v1());
      assertEquals(new ContentHandle("test:foo/create", 1), p.getVariants()[0].getHandle());
    }

    {
      final var p = mapper.relaxed().capture(new CreateBar_v1());
      assertEquals(new ContentHandle("test:bar/create", 1), p.getVariants()[0].getHandle());
    }
  }

  @Test
  public void testCompactRelaxedCapturePoly_normalWithOne() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);

    final var p = mustBeSubtype(mapper.compactRelaxed().capture(new Object[] { new CreateFoo_v0() }), MonoVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:foo/create", 0), p.getHandle());
  }

  @Test
  public void testCompactRelaxedCapturePoly_normalWithMany() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);

    final var mp = mustBeSubtype(mapper.compactRelaxed().capture(new CreateFoo_v0(), new CreateBar_v1()), PolyVariant.class, AssertionError::new);
    assertEquals(2, mp.getVariants().length);

    final var p0 = mp.getVariants()[0];
    assertTrue(p0.getContent() instanceof CreateFoo_v0);
    assertEquals(new ContentHandle("test:foo/create", 0), p0.getHandle());

    final var p1 = mp.getVariants()[1];
    assertTrue(p1.getContent() instanceof CreateBar_v1);
    assertEquals(new ContentHandle("test:bar/create", 1), p1.getHandle());
  }

  @Test
  public void testStrictCaptureUni_normal() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    final var mp = mapper.strict().capture(new CreateBar_v0());
    assertEquals(1, mp.getVariants().length);
    assertEquals(new ContentHandle("test:bar/create", 0), mp.getVariants()[0].getHandle());
  }

  @Test
  public void testStrictCaptureUni_missingMapping() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.strict().capture(new CreateBar_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateBar_v1.class);
  }

  @Test
  public void testStrictCaptureUni_insufficientVariants() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.strict().capture(new CreateFoo_v1());
    }).isInstanceOf(InsufficientVariantsException.class).hasMessage("Insufficient variants supplied; expected: 2, got: 1");
  }

  @Test
  public void testStrictCapturePoly_normalWithOne() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    final var mp = mapper.strict().capture(new Object[] { new CreateBar_v0() });
    assertEquals(1, mp.getVariants().length);
    assertEquals(new ContentHandle("test:bar/create", 0), mp.getVariants()[0].getHandle());
  }

  @Test
  public void testStrictCapturePoly_normalWithMany() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    final var mp = mapper.strict().capture(new Object[] { new CreateFoo_v1(), new CreateFoo_v0() });
    assertEquals(2, mp.getVariants().length);
    assertEquals(new ContentHandle("test:foo/create", 1), mp.getVariants()[0].getHandle());
    assertEquals(new ContentHandle("test:foo/create", 0), mp.getVariants()[1].getHandle());
  }

  @Test
  public void testStrictCapturePoly_nonDecreasingVersion() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.strict().capture(new Object[] { new CreateFoo_v0(), new CreateFoo_v1() });
    })
    .isInstanceOf(NonDecreasingContentVersionsException.class)
    .hasMessage("Content items should be arranged in decreasing order of version; v1 at index 1 is later than v0 at index 0");
  }

  @Test
  public void testStrictCapturePoly_mixedContentTypes() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.strict().capture(new Object[] { new CreateFoo_v1(), new CreateBar_v0() });
    })
    .isInstanceOf(MixedContentTypesException.class)
    .hasMessage("Mixed content types unsupported; expected: test:foo/create at index 1, got: test:bar/create");
  }

  @Test
  public void testCompactStrictCaptureUni_normal() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    final var p = mustBeSubtype(mapper.compactStrict().capture(new CreateBar_v0()), MonoVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:bar/create", 0), p.getHandle());
  }

  @Test
  public void testCompactStrictCaptureUni_missingMapping() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.compactStrict().capture(new CreateBar_v1());
    }).isInstanceOf(NoSuchMappingException.class).hasMessage("No mapping for " + CreateBar_v1.class);
  }

  @Test
  public void testCompactStrictCaptureUni_insufficientVariants() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.compactStrict().capture(new CreateFoo_v1());
    }).isInstanceOf(InsufficientVariantsException.class).hasMessage("Insufficient variants supplied; expected: 2, got: 1");
  }

  @Test
  public void testCompactStrictCapturePoly_normalWithOne() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    final var p = mustBeSubtype(mapper.compactStrict().capture(new Object[] { new CreateBar_v0() }), MonoVariant.class, AssertionError::new);
    assertEquals(new ContentHandle("test:bar/create", 0), p.getHandle());
  }

  @Test
  public void testCompactStrictCapturePoly_normalWithMany() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    final var mp = mustBeSubtype(mapper.compactStrict().capture(new Object[] { new CreateFoo_v1(), new CreateFoo_v0() }), PolyVariant.class, AssertionError::new);
    assertEquals(2, mp.getVariants().length);
    assertEquals(new ContentHandle("test:foo/create", 1), mp.getVariants()[0].getHandle());
    assertEquals(new ContentHandle("test:foo/create", 0), mp.getVariants()[1].getHandle());
  }

  @Test
  public void testCompactStrictCapturePoly_nonDecreasingVersion() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.compactStrict().capture(new Object[] { new CreateFoo_v0(), new CreateFoo_v1() });
    })
    .isInstanceOf(NonDecreasingContentVersionsException.class)
    .hasMessage("Content items should be arranged in decreasing order of version; v1 at index 1 is later than v0 at index 0");
  }

  @Test
  public void testCompactStrictCapturePoly_mixedContentTypes() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    Assertions.assertThatThrownBy(() -> {
      mapper.compactStrict().capture(new Object[] { new CreateFoo_v1(), new CreateBar_v0() });
    })
    .isInstanceOf(MixedContentTypesException.class)
    .hasMessage("Mixed content types unsupported; expected: test:foo/create at index 1, got: test:bar/create");
  }

  private static MonoVariant emulatePacked(String contentType, int contentVersion, Object content) {
    return new MonoVariant(new ContentHandle(contentType, contentVersion), new IdentityPackedForm(content), null);
  }

  @Test
  public void testMap_uni_matching() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);

    {
      final var v0 = new CreateFoo_v0();
      final var p = emulatePacked("test:foo/create", 0, v0);
      assertTrue(p.getPacked() instanceof IdentityPackedForm);
      assertNull(p.getContent());
      assertSame(v0, mapper.map(p));
    }
    {
      final var v1 = new CreateFoo_v1();
      final var p = emulatePacked("test:foo/create", 1, v1);
      assertSame(v1, mapper.map(p));
    }
    {
      final var v0 = new CreateBar_v0();
      final var p = emulatePacked("test:bar/create", 0, v0);
      assertSame(v0, mapper.map(p));
    }
  }

  @Test
  public void testMap_uni_noMatchingContentType() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class);

    final var v0 = new CreateBar_v0();
    final var p = emulatePacked("test:bar/create", 0, v0);
    assertNull(mapper.map(p));
  }

  @Test
  public void testMap_uni_noMatchingLegacyVersion() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withVersion("test:foo/create", 0, CreateFoo_v0.class);

    final var v1 = new CreateFoo_v1();
    final var p = emulatePacked("test:foo/create", 1, v1);
    assertNull(mapper.map(p));
  }

  @Test
  public void testMap_uni_obsoletedVersion() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withVersion("test:foo/create", 1, CreateFoo_v1.class);

    final var v0 = new CreateFoo_v0();
    final var p = emulatePacked("test:foo/create", 0, v0);
    assertNull(mapper.map(p));
  }

  @Test
  public void testMap_uni_noUnpacker() {
    final var mapper = new ContentMapper()
        .withVersion("test:foo/create", 1, CreateFoo_v1.class);

    final var v1 = new CreateFoo_v1();
    final var p = emulatePacked("test:foo/create", 1, v1);
    Assertions.assertThatThrownBy(() -> {
      mapper.map(p);
    }).isInstanceOf(IllegalStateException.class).hasMessage("No unpacker for " + IdentityPackedForm.class);
  }

  @Test
  public void testMap_multi_matchingFirst() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class)
        .withVersion("test:foo/create", 1, CreateFoo_v1.class)
        .withVersion("test:bar/create", 1, CreateBar_v1.class);

    final var v1 = new CreateFoo_v1();
    final var v0 = new CreateFoo_v0();
    final var mp = new PolyVariant(emulatePacked("test:foo/create", 1, v1), emulatePacked("test:foo/create", 0, v0));
    assertSame(v1, mapper.map(mp));
  }

  @Test
  public void testMap_multi_matchingFallback() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker())
        .withVersion("test:foo/create", 0, CreateFoo_v0.class)
        .withVersion("test:bar/create", 0, CreateBar_v0.class);

    final var v1 = new CreateFoo_v1();
    final var v0 = new CreateFoo_v0();
    final var mp = new PolyVariant(emulatePacked("test:foo/create", 1, v1), emulatePacked("test:foo/create", 0, v0));
    assertSame(v0, mapper.map(mp));
  }

  @Test
  public void testMap_multi_obsoleted() {
    final var mapper = new ContentMapper()
        .withUnpacker(new IdentityUnpacker());

    final var v1 = new CreateFoo_v1();
    final var v0 = new CreateFoo_v0();
    final var mp = new PolyVariant(emulatePacked("test:foo/create", 1, v1), emulatePacked("test:foo/create", 0, v0));
    assertNull(mapper.map(mp));
  }
}
