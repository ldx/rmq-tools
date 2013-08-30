-module(sender).

-export([start_link/1, send/1]).

-define(INTERVAL, 25).
-define(TIMEOUT_INTERVAL, 500).

wait_for_server(Try, MaxTries) when Try >= MaxTries ->
    io:format("."),
    io:format("timeout~n"),
    timeout;

wait_for_server(Try, MaxTries) ->
    io:format("."),
    Result = rmq_publish_server:ready(),
    case Result of
        ok -> io:format("ready~n"),
              ok;
        not_ok -> timer:sleep(?TIMEOUT_INTERVAL),
                  wait_for_server(Try + 1,
                                  MaxTries)
    end.

wait_for_server(Timeout) ->
    io:format("~nwaiting for acknowledgements"),
    MaxTries = Timeout * 1000.0 / ?TIMEOUT_INTERVAL,
    wait_for_server(0, MaxTries).

send_file(File) ->
    case file:read_file(File) of
        {ok, Content} -> rmq_publish_server:send(Content);
        {error, Reason} -> Error = io_lib:format("error reading ~s: ~s~n",
                                                 [File, Reason]),
                           error(Error)
    end.

send_files([]) ->
    ok;

send_files([H|T]) ->
    send_file(H),
    send_files(T).

tick() ->
    receive
        tick -> ok
    end,
    erlang:send_after(?INTERVAL, self(), tick).

calculate_dps(N, Start) ->
    T = timer:now_diff(now(), Start) div 1000,
    case T of
        0 -> 0.0;
        X -> N / (X / 1000.0)
    end.

loop(Files, _Start, Counter, _Dps) when Counter >= length(Files) ->
    ok;

loop(Files, Start, Counter, Dps) ->
    CurrentDps = calculate_dps(Counter, Start),
    tick(),
    T = timer:now_diff(now(), Start) div 1000,
    M = round((Dps * T) / 1000) - Counter,
    N = if
            Counter + M > length(Files) -> length(Files) - Counter;
            Counter + M =< length(Files), M >= 0 -> M;
            Counter + M =< length(Files), M < 0 -> 0
        end,
    send_files(lists:sublist(Files, Counter + 1, N)),
    NewCounter = Counter + N,
    io:format("published ~p files, DPS ~.1f\r", [NewCounter, CurrentDps]),
    loop(Files, Start, NewCounter, Dps).

loop(Files, Dps) ->
    erlang:send_after(?INTERVAL, self(), tick),
    loop(Files, now(), 0, Dps).

get_filelist([], List) ->
    List;

get_filelist([H|T], List) ->
    Result = file:list_dir(H),
    NewList = case Result of
                  {ok, Filenames} -> [filename:join(H, F) || F <- Filenames];
                  {error, Reason} -> io:format("error accessing ~s: ~s~n",
                                               [H, Reason]),
                                     []
              end,
    get_filelist(T, List ++ NewList).

get_filelist(Dirs) ->
    get_filelist(Dirs, []).

send(Dirs, Files, Dps, Timeout) ->
    DirList = get_filelist(Dirs),
    loop(Files ++ DirList, Dps),
    wait_for_server(Timeout),
    application:stop(rmq_publish).

send(Args) ->
    Dps = proplists:get_value(dps, Args),
    FileList = proplists:get_all_values(file, Args),
    DirList = proplists:get_all_values(directory, Args),
    Timeout = proplists:get_value(timeout, Args),
    send(DirList, FileList, Dps, Timeout).

start_link(Args) ->
    Pid = spawn_link(?MODULE, send, [Args]),
    {ok, Pid}.
