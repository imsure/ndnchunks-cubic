# ndncatchunks and ndnputchunks

**ndncatchunks** and **ndnputchunks** are a pair of programs to transfer a file as Data segments.

* **ndnputchunks** is a producer program that reads a file from the standard input, and makes
  it available as NDN Data segments.  It appends version and segment number components
  to the specified name, according to the
  [NDN naming conventions](http://named-data.net/publications/techreports/ndn-tr-22-ndn-memo-naming-conventions/).

* **ndncatchunks** is a consumer program that fetches Data segments of a file, optionally
  discovering the latest version of the file, and writes the content of the retrieved file to
  the standard output.

## Version discovery methods in ndncatchunks

* `fixed`    : sends an interest attempting to find a data packet with the
               specified prefix and version number. A version component must be present at the
               end of the user-specified NDN name.

* `iterative`: sends a series of interests with ChildSelector set to prefer the
               rightmost child and Exclude selectors, attempting to find a data packet with the
               specified prefix and the latest (the largest in the NDN canonical ordering)
               version number.  The version is declared "latest" after a predefined number of
               data retrieval timeouts (default: 1).

The default discovery method is `iterative`.

## Interest pipeline types in ndncatchunks

* `fixed`: maintains a fixed-size window of Interests in flight; the window size is configurable
           via a command line option and defaults to 1.

* `aimd` : sends Interests using an additive-increase/multiplicative-decrease (AIMD) algorithm to
           control the window size. By default, a Conservative Loss Adaptation algorithm is adopted
           combining with the AIMD algorithm, that is, at most one window decrease will be
           performed per round-trip-time. For details please refer to:
  [A Practical Congestion Control Scheme for Named Data
  Networking](https://www.researchgate.net/publication/306259672_A_Practical_Congestion_Control_Scheme_for_Named_Data_Networking)

The default Interest pipeline type is `fixed`.

## Usage examples

### Publishing

The following command will publish the text of the GPL-3 license under the `/localhost/demo/gpl3`
prefix:

    ndnputchunks ndn:/localhost/demo/gpl3 < /usr/share/common-licenses/GPL-3

To find the published version you have to start ndnputchunks with the `-p` command line option,
for example:

    ndnputchunks -p ndn:/localhost/demo/gpl3 < /usr/share/common-licenses/GPL-3

This command will print the published version to the standard output.

To publish data with a specific version, you must append a version component to the end of the
prefix. The version component must follow the aforementioned NDN naming conventions.
For example, the following command will publish the version `%FD%00%00%01Qc%CF%17v` of the
`/localhost/demo/gpl3` prefix:

    ndnputchunks ndn:/localhost/demo/gpl3/%FD%00%00%01Qc%CF%17v < /usr/share/common-licenses/GPL-3

If the version component is not valid, a new well-formed version will be generated and appended
to the supplied NDN name.


### Retrieval

To retrieve the latest version of a published file, the following command can be used:

    ndncatchunks -d iterative ndn:/localhost/demo/gpl3

This command will use the iterative method to discover the latest version of the file.

To fetch a specific version of a published file, you can use the `fixed` version discovery method.
In this case the version needs to be supplied as part of the name. For example, if the version
is known to be `%FD%00%00%01Qc%CF%17v`, the following command will fetch that exact version of the
file:

    ndncatchunks -d fixed ndn:/localhost/demo/gpl3/%FD%00%00%01Qc%CF%17v


For more information, run the programs with `--help` as argument.
