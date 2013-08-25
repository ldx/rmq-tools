%% rmq_publish escript entry

-module(rmq_publish).  %% needed for rebar

-export([main/1]).

-define(PROG, atom_to_list(?MODULE)).

set_env(_App, []) ->
    ok;

set_env(App, [{Key, Val}|Rest]) ->
    application:set_env(App, Key, Val),
    set_env(App, Rest).

parse_headers([], Headers) ->
    Headers;

parse_headers([H|T], Headers) ->
    [Key, Value] = string:tokens(H, "="),
    Header = [{list_to_binary(Key), binary, list_to_binary(Value)}],
    parse_headers(T, Headers ++ Header).

parse_headers(Params) ->
    parse_headers(Params, []).

start(Props) ->
    io:format("starting with parameters ~p~n", [Props]),
    ok = application:load(rmq_publish),
    ok = set_env(rmq_publish, Props),
    ok = application:start(rmq_publish),
    Pid = erlang:whereis(rmq_publish_sup),
    Ref = erlang:monitor(process, Pid),
    receive
        {'DOWN', Ref, process, Pid, shutdown} ->
            halt(0);
        {'DOWN', Ref, process, Pid, Reason} ->
            io:format("error: ~p~n", [Reason]),
            halt(1);
        Msg ->
            io:format("unexpected message: ~p~n", [Msg]),
            halt(-1)
    end.

main(Args) ->
    OptSpecList =
    [{uri, $u, "uri", {string, "amqp://guest:guest@localhost:5672/%2f"},
      "RabbitMQ AMQP URI."},
     {exchange, $e, "exchange", binary, "Name of exchange to publish into."},
     {routing_key, $r, "routing_key", binary, "Routing key to publish with."},
     {dps, $s, "dps", {integer, 10}, "Number of docs per second to send."},
     {timeout, $t, "timeout", {integer, 60}, "Timeout to wait for outstanding"
      "acknowledgements when finished, in seconds."},
     {verbose, $v, "verbose", integer, "Verbosity level."},
     {directory, $d, "directory", string, "Directory from which to publish "
      "all files. Can be used multiple times."},
     {file, $f, "file", string, "File to publish. Can be used multiple "
      "times."},
     {header, undefined, "header", string, "Add AMQP header for every "
      "message. Example: --header myheader=myvalue."},
     {help, $h, "help", undefined, "Show usage info."}],
    {ok, {Props, Leftover}} = getopt:parse(OptSpecList, Args),
    Help = proplists:get_value(help, Props),
    Headers = parse_headers(proplists:get_all_values(header, Props)),
    Props1 = Props ++ [{headers, Headers}],
    if Help =/= undefined; length(Leftover) =/= 0 -> getopt:usage(OptSpecList,
                                                                  ?PROG),
                                                     exit(normal);
       Help =:= undefined, length(Leftover) =:= 0 -> start(Props1)
    end.
