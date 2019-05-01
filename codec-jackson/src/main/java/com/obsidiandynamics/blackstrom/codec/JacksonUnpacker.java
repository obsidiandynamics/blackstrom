package com.obsidiandynamics.blackstrom.codec;

import com.obsidiandynamics.func.*;

public final class JacksonUnpacker implements Unpacker<JacksonPackedForm> {
  private static final JacksonUnpacker INSTANCE = new JacksonUnpacker();
  
  public static JacksonUnpacker getInstance() { return INSTANCE; }
  
  private JacksonUnpacker() {}
  
  @Override
  public Class<? extends JacksonPackedForm> getPackedType() {
    return JacksonPackedForm.class;
  }
  
  @Override
  public Object unpack(JacksonPackedForm packed, Class<?> contentClass) {
    return Exceptions.wrap(() -> packed.getParser().getCodec().treeToValue(packed.getNode(), contentClass), 
                           RuntimeJsonProcessingException::new);
  }
}
