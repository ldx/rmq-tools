%% rmq_consume escript entry

-module(rmq_consume). % needed for rebar

-export([main/1]).

-define(PROG, atom_to_list(?MODULE)).

%% ===================================================================
%% API
%% ===================================================================

main(Args) ->
    OptSpecList =
    [{uri, $u, "uri", {string, "amqp://guest:guest@localhost:5672/%2f"},
      "RabbitMQ AMQP URI."},
     {queue, $q, "queue", string, "Queue to consume from. Mandatory."},
     {verbose, $v, "verbose", integer, "Verbosity level."},
     {directory, $d, "directory", {string, "."}, "Directory to which save "
      "files to."},
     {timeout, $t, "timeout", {integer, 0}, "Timeout waiting for messages "
      "from queue, in seconds. When no new message has been received for "
      "this amount of time, " ++ ?PROG ++ " exits. 0 means running forever."},
     {help, $h, "help", undefined, "Show usage info."}],
    {ok, {Props, Leftover}} = getopt:parse(OptSpecList, Args),
    Help = proplists:get_value(help, Props),
    if Help =/= undefined; length(Leftover) =/= 0 -> getopt:usage(OptSpecList,
                                                                  ?PROG),
                                                     exit(normal);
       Help =:= undefined, length(Leftover) =:= 0 -> start(Props)
    end.

%% ===================================================================
%% Private
%% ===================================================================

set_env(_App, []) ->
    ok;

set_env(App, [{Key, Val}|Rest]) ->
    application:set_env(App, Key, Val),
    set_env(App, Rest).

start(Props) ->
    io:format("starting with parameters ~p~n", [Props]),
    ok = application:load(rmq_consume),
    ok = set_env(rmq_consume, Props),
    ok = application:start(rmq_consume),
    Pid = erlang:whereis(rmq_consume_sup),
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
