rmq-tools
=========

These are tools for publishing/consuming messages to/from RabbitMQ.

Build
-----
Since the tools are written in Erlang you need an Erlang installation (tested with R14 and R16) and [rebar](https://github.com/basho/rebar).

The applications are contained in git submodules, so first make sure they are up to date:

    $ git submodule sync; git submodule update --init

Then build them one by one:

    $ cd ../rmq_consume
    $ rebar get-deps compile escriptize
    [...]
    $ cd rmq_publish
    $ rebar get-deps compile escriptize
    [...]
    $ cd ../rmq_shovel
    $ rebar get-deps compile escriptize
    [...]

This should fetch dependencies, compile everything, and create executable escripts.

You can also use `make` to drive the build and build everything at once:

    $ make

in the top level directory should do that.

Usage
-----
Please see the READMEs of the applications.
