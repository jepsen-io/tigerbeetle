// A java.util.random which draws from Antithesis' SDK.

package jepsen.tigerbeetle;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AntithesisRandom extends java.util.Random {
  // See https://github.com/openjdk/jdk/blob/master/src/java.base/share/classes/java/util/Random.java

  public static List<Boolean> booleans = Arrays.asList(true, false);

  private final com.antithesis.sdk.Random r;

  public AntithesisRandom() {
    super();
    this.r = new com.antithesis.sdk.Random();
  }

  // Most everything in j.u.Random sources entropy from here. See the
  // j.u.Random docstring for semantics.
  protected int next(int bits) {
    // I am hoping, though figuring this out will take time, that all 64 bits
    // of the Antithesis' Random are uniformly (whatever that means in a guided
    // search) distributed between 0 and 1. We therefore need to clip off 64 -
    // bits bits to get the right range.
    return (int)(r.getRandom() >>> (64 - bits));
  }

  public long nextLong() {
    return r.getRandom();
  }

  public boolean nextBoolean() {
    return r.randomChoice(booleans);
  }
}
