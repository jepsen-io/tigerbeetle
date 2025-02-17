# jepsen.tigerbeetle

Jepsen tests for the TigerBeetle distributed database.

## Installation

You'll need a [Jepsen environment](https://github.com/jepsen-io/jepsen?tab=readme-ov-file#setting-up-a-jepsen-environment).

## Quickstart

Use `lein run test-all` to run a relatively comprehensive battery of tests. `lein run test --help` will print out a full list of tuning parameters.

Use `lein repl` to get a REPL shell with a bunch of useful utilities.
`(store/latest)` will grab the most recent test, if you'd like to start
introspecting it, or use `store/test` with a path or index (-1 means latest) to
load a specific test.

## Known Limitations

TigerBeetle does not currently survive certain kinds of faults. For example,
snapshot-and-restore disk faults (`--nemesis snapshot-file-chunks,kill`) can
cause 0.16.27 to panic on every startup with a `reached unreachable code`,
possible due to unexpected superblock rollback. The cluster as a whole can
still run, so long as these faults are limited to a minority of nodes.

Helical faults in the WAL can permanently disable a TB cluster. The test
harness tries to avoid this: by default, it causes only superblock, client
replies, and grid corruption when using `--nemesis-file-targets helix`. You can
explicitly override this with `--nemesis-file-zones wal`.

## Internal Tests

There is an internal test suite which validates that the model, checker, and
generator behave as expected. This is not comprehensive--it's designed to get
quick feedback during development, to validate internal components, and does
not focus on testing behavior that would manifest as something going wrong
during an actual test run.

```
lein test
```

## Interpreting Faults

Unlike most Jepsen tests, this test suite does something unusual--it
reconstructs the order of all known-committed transactions from the history,
then replays them against an executable model (`jepsen.tigerbeetle.model`). This will be extremely wrong if for some reason the test *can't* finish the final read phase. If this happens, you'll get a result like:

```clj
{:valid?                  :unknown
 :final-reads-incomplete? true}
```

If you see a model error in a test, it means that the model predicted a
different outcome from applying a specific operation. The state of the model
could depend on the entire history up until that operation, and the model state
itself can be extremely large. Both of these make it difficult to debug model
errors.

There are several useful REPL utilities in `jepsen.tigerbeetle.repl`, which are available as soon as you open `lein repl`. One particularly useful power is stepping through each operation and its resulting model states. For example:

```clj
; Load a specific test run and its history
=> (def t (store/test "/home/aphyr/tigerbeetle/store/0.16.26 transfer c=all /20250211T171636.094-0600")) (def h (:history t))
nil
; Produce a lazy sequence of maps:
; {:op      the invocation
;  :op'     the completion
;  :model'  a debugging-friendly view of the model resulting from this op}
=> (def steps (debug-model-steps h))
nil
; Inspect a specific step's model
(pprint (:model' (nth steps 1234)))
```

Want to see where an ID was created? Here's the [invoke complete] pair of ops
responsible:

```clj
=> (find-create 123N h)
[op op']
```

How about every time an ID was read, either by ID or returned by a predicate
query? For convenience, the :values of these ops are restricted to just that
specific ID, so you don't have to dig through thousands of events.

```clj
=> (reads-of 123N h)
[[op op'] ...]
```

Or all attempted transfers which debited a specific account, broken down by
op completion type?

```clj
=> (transfers-debiting 123N h)
{:ok   [t1 t2 ...],
 :info [t1 t2 ...]
 :fail [t1 t2 ...]}

If you wind up debugging generators, it can be particularly helpful to
reconstruct what the generator state would have been at a specific point in the
history. Here's the result of the first 15 ops in test `t`:

```clj
=> (reconstruct-generator t (take 15 (:history t)))
GenContext{...}
```

What transfers did we know about at that time?

```clj
=> (->> (take 15 (:history t)) (reconstruct-generator t) :state :transfers lm/debug pprint)
{:seen     (),
 :likely   (),
 :unlikely (1N 2N 3N ...)}
```

## License

Copyright © 2024, 2025 Jepsen, LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
