package jepsen.tigerbeetle;

// A backtracking search implementation of the Subset Sum Problem (SSP) over
// unsigned 128-bit integers. Given an array of 128-bit integers `xs` and a
// nonzero target sum t, we build an array `state` of length n, such that for
// every non-zero `state[i]`, a subset of {x0, x1, ... xi} sums to `state[i]`.
// We treat `state` as the stack of a backtracking search, where all but the last `state[i]` have two successors:
//
// state[i+1] = state[i]            Meaning that we did not add element i
// state[i+1] = state[i] + xs[i+i]  Meaning that we did add element i
//
// Two assumptions: 1, most xs get used. 2, earlier xs are more likely than
// later xs. Our search is depth-first, taking every possible element until we
// have exceeded t. Denoting included elements as +:
//
// |     |
// |+    |
// |++   |
// |+++  |
// |++++ | Exceeded!
//
// Then we backtrack, decrementing i until we see a change: state[i-1] !=
// state[i]. At this juncture we set state[i] = state[i-1], saying "what if we
// *didn't* take this element", and zip back forward, taking every element
// again.
//
// |++   | Don't take x[2]
// |++ + | Zip forward
// |++ ++| Exceeded!
//
// This is the logical structure of our search, but for performance, we have a
// few optimizations:
//
// - Since Java has no native 128-bit integer type, we use an flat array of
// 64-bit longs, treating them as [high0 low0 high1 low1 ...]. Indices into
// these arrays are always even.
//
// - To avoid branch expressions, we insert a special zero element at the start
// of the state vector. xs[i] corresponds to state[(i+1)*2].
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.Iterable;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SubsetSum {
    private static final VarHandle BIG_ENDIAN_LONG_VIEW = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  // Utility math on 128-bit integers

  public static BigInteger MAX_VALUE =
    BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE);

  // Computes the high bits carried from adding low bits a and b
  public static long carry(long a, long b) {
    return ((a >>> 1) + (b >>> 1) + ((a & b) & 1)) >>> 63;
  }

  // For addition, returns the low bits resulting from adding a and b.
  public static long addLow(long aLow, long bLow) {
    return aLow + bLow;
  }

  // For addition, returns the high bits resulting from adding a and b.
  public static long addHigh(long aHigh, long aLow, long bHigh, long bLow) {
    return aHigh + bHigh + carry(aLow, bLow);
  }

  // Are integers a and b equal?
  public static boolean equals(long aHigh, long aLow, long bHigh, long bLow) {
    // Guessing that most figures differ in their low bits only
    return (aLow == bLow) && (aHigh == bHigh);
  }

  // Comparator for 128-bit ints
  public static int compare(long aHigh, long aLow, long bHigh, long bLow) {
    int result = Long.compareUnsigned(aHigh, bHigh);
    if (0 == result) {
      return Long.compareUnsigned(aLow, bLow);
    }
    return result;
  }

  // Is a less than b?
  public static boolean lt(long aHigh, long aLow, long bHigh, long bLow) {
    return (compare(aHigh, aLow, bHigh, bLow) < 0);
  }

  // BigInteger -> longs
  public static long low(final BigInteger x) {
    return x.longValue();
  }

  public static long high(final BigInteger x) {
    if (MAX_VALUE.compareTo(x) < 0) {
      throw new ArithmeticException("Value larger than 2^128 - 1: " + x);
    }
    return x.shiftRight(64).longValue();
  }

  // longs -> BigInteger
  public static BigInteger bigInteger(long high, long low) {
    byte[] bytes = new byte[16];
    BIG_ENDIAN_LONG_VIEW.set(bytes, 0, high);
    BIG_ENDIAN_LONG_VIEW.set(bytes, Long.BYTES, low);
    return new BigInteger(1, bytes);
  }

  // Our target integer, broken up into a pair of longs
  public final long tHigh;
  public final long tLow;

  // The raw xs (2 indices per x)
  public long[] xs;

  // Our state array (2 indices per x)
  public long[] state;

  // And our currently considered index into the state (to the high long)
  public int index;

  // Main API; handles special cases like empty xs and 0 target. Returns a list
  // of BigIntegers in `xs` summing to `t`, or `null` if none found.
  public static List<BigInteger> solve(final BigInteger t, final List<BigInteger> xs) {
    // Trivial case: empty lists always sum to zero.
    if (t.equals(BigInteger.ZERO)) {
      return new ArrayList<BigInteger>();
    }
    // Trivial case: nonzero target, empty list, no dice
    if (xs.size() == 0) {
      return null;
    }

    // Strip out zeroes
    final List<BigInteger> nonZeroXs = xs.stream()
      .filter(x -> !BigInteger.ZERO.equals(x))
      .collect(Collectors.toList());

    // Validate that their total is in bounds
    BigInteger total = BigInteger.ZERO;
    for (BigInteger x : xs) {
      total = total.add(x);
    }
    if (MAX_VALUE.compareTo(total) < 0) {
      throw new ArithmeticException("Total of all xs is greater than 2^128 - 1: " + total);
    }

    final SubsetSum s = new SubsetSum(t, nonZeroXs);
    return s.solve();
  }

  // Construct a solver. We take a target BigInteger `t` and a List of summands
  // `xs`. t must be non-zero. xs must be non-empty, and contain no zeroes.
  public SubsetSum(final BigInteger t, final List<BigInteger> xs) {
    this.tHigh = high(t);
    this.tLow  = low(t);
    //System.out.println("Target " + t + " broken into " + tHigh + ", " + tLow);
    if (equals(0, 0, tHigh, tLow)) {
      throw new IllegalArgumentException("Expected non-zero target, got " +
          t + " -> " + tHigh + ", " + tLow);
    }

    // System.out.println("xs " + xs);

    // Slurp xs into array
    this.xs = new long[xs.size() * 2];
    final Iterator<BigInteger> iter = xs.iterator();
    int i = 0;
    BigInteger x;
    while (iter.hasNext()) {
      x = iter.next();
      this.xs[i] = high(x);
      i++;
      this.xs[i] = low(x);
      i++;
    }

    // Our state vector is one (pair of longs) larger--it includes a special
    // initial 0.
    this.state = new long[(this.xs.length + 2)];

    // We start our search at index 0.
    this.index = 0;
  }

  // Does this state vector, at index i, reflect having us having taken the
  // corresponding x at xs[i - 2]?
  public boolean contains(int i) {
    return !equals(state[i - 2], state [i - 1], state[i], state[i + 1]);
  }

  // We're solved iff state[index] is equal to t.
  public boolean isSolved() {
    return equals(tHigh, tLow, state[index], state[index + 1]);
  }

  // We're too high if state[index] is beyond t
  public boolean tooHigh() {
    return lt(tHigh, tLow, state[index], state[index + 1]);
  }

  // Turns the current state vector into a list of xs.
  public List<BigInteger> toBigIntegers() {
    ArrayList<BigInteger> out = new ArrayList<BigInteger>();
    long prevHigh = 0;
    long prevLow = 0;
    for (int i = 2; i <= index; i += 2) {
      if (contains(i)) {
        out.add(bigInteger(xs[i - 2], xs[i - 1]));
      }
    }
    return out;
  }

  // Zips forward, taking the next index, then the next, etc, until we're
  // too high or hit the end. Returns a solution if found, or null.
  public List<BigInteger> forward() {
    while (index < (state.length - 2)) {
      //System.out.println("Forward " + this);
      index += 2;
      state[index] = addHigh(state[index - 2], state[index - 1],
                             xs[index - 2],    xs[index - 1]);
      state[index + 1] = addLow(state[index - 1], xs[index - 1]);
      if (isSolved()) {
        return toBigIntegers();
      }
      if (tooHigh()) {
        //System.out.println("Too high (wanted " + bigInteger(tHigh, tLow) + ") " + this);
        return null;
      }
    }
    //System.out.println("Forward' " + this);
    return null;
  }

  // Backtracks until we reach a point where the state contained the x, and
  // remove that x from the state. Returns a solution if found, or null.
  public List<BigInteger> back() {
    while (0 < index) {
      //System.out.println("Back " + this);
      if (contains(index)) {
        // We took this element in the previous search. Try the other branch.
        //System.out.println("Flipping " + index);
        state[index] = state[index - 2];
        state[index + 1] = state[index - 1];
        if (isSolved()) {
          return toBigIntegers();
        }
        return null;
      } else {
        index -= 2;
      }
    }
    return null;
  }

  // Searches for a solution, returning either a list of BigIntegers or null if
  // no solution found.
  public List<BigInteger> solve() {
    //System.out.println("Initial state: " + this);
    // First step forward
    List<BigInteger> solution = forward();
    if (solution != null) {
      return solution;
    }

    // Alternate stepping back and forth, looking for solutions, until we come
    // back to index 0.
    while (true) {
      solution = back();
      if (solution != null) {
        return solution;
      }
      if (index == 0) {
        // Rewound to the beginning
        return null;
      }
      solution = forward();
      if (solution != null) {
        return solution;
      }
    }
  }

  public String toString() {
    List<String> stateL = new ArrayList<String>();
    for (int i = 0; i < state.length; i += 2) {
      stateL.add(bigInteger(state[i], state[i+1]).toString());
    }
    return "(:i " + index + ", :state [" + String.join(" ", stateL) + "])";
  }
}
