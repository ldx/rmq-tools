%% rmq_publish escript entry

-module(rmq_publish).  % needed for rebar

-export([main/1]).

-define(PROG, atom_to_list(?MODULE)).

%% ===================================================================
%% API
%% ===================================================================

main(Args) ->
    OptSpecList =
    [{uri, $u, "uri", {string, "amqp://guest:guest@localhost:5672/%2f"},
      "RabbitMQ AMQP URI."},
     {exchange, $e, "exchange", binary, "Name of exchange to publish into."},
     {routing_key, $r, "routing_key", binary, "Routing key to publish with."},
     {dps, $s, "dps", {integer, 10}, "Number of docs per second to send."},
     {timeout, $t, "timeout", {integer, 60}, "Timeout to wait for outstanding"
      "acknowledgements when finished, in seconds."},
     {directory, $d, "directory", string, "Directory from which to publish "
      "all files. Can be used multiple times."},
     {file, $f, "file", string, "File to publish. Can be used multiple "
      "times."},
     {tarball, $b, "tarball", string, "Tarball of files to publish. The "
      "tarball will be extracted to memory, and the files in it published. "
      "Can be used multiple times."},
     {check_queue, $c, "check_queue", string, "Check if the size of a queue "
      "exceeds a certain limit. Example: --check_queue queuename=maxsize. "
      "Can be used multiple times."},
     {header, undefined, "header", string, "Add AMQP header for every "
      "message. Example: --header myheader=myvalue."},
     {immediate, undefined, "immediate", boolean, "Set immediate flag."},
     {mandatory, undefined, "mandatory", boolean, "Set mandatory flag."},
     {version, $v, "version", undefined, "Show version info."},
     {help, $h, "help", undefined, "Show usage info."}],
    {ok, {Props, Leftover}} = getopt:parse(OptSpecList, Args),
    Help = proplists:get_value(help, Props),
    Headers = parse_headers(proplists:get_all_values(header, Props)),
    Queues = parse_queues(proplists:get_all_values(check_queue, Props)),
    Props1 = Props ++ [{headers, Headers}] ++ [{queues, Queues}],
    Version = proplists:get_value(version, Props),
    case Version of
        undefined -> ok;
        _ -> show_version(),
             halt(0)
    end,
    if Help =/= undefined; length(Leftover) =/= 0 -> getopt:usage(OptSpecList,
                                                                  ?PROG),
                                                     halt(0);
       Help =:= undefined, length(Leftover) =:= 0 -> start(Props1)
    end.

%% ===================================================================
%% Private
%% ===================================================================

show_version() ->
    ok = application:load(rmq_publish),
    {ok, Vsn} = application:get_key(rmq_publish, vsn),
    io:format("rmq_publish v~ts~n", [Vsn]).

parse_headers([], Headers) ->
    Headers;

parse_headers([H|T], Headers) ->
    [Key, Value] = string:tokens(H, "="),
    Header = [{Key, longstr, Value}],
    parse_headers(T, Headers ++ Header).

parse_headers(Params) ->
    parse_headers(Params, []).

parse_queues([], Acc) ->
    Acc;

parse_queues([H|T], Acc) ->
    [Queue, MaxSize] = string:tokens(H, "="),
    parse_queues(T, [{Queue, list_to_integer(MaxSize)}|Acc]).

parse_queues(Params) ->
    parse_queues(Params, []).

start(Props) ->
    io:format("starting with parameters ~p~n", [Props]),
    ok = application:load(rmq_publish),
    ok = application:set_env(rmq_publish, props, Props),
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
