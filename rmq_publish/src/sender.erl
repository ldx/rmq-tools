-module(sender).

-export([start_link/1, send/1]).

-define(INTERVAL, 25).
-define(TIMEOUT_INTERVAL, 500).
-define(MAX_DPS_SAMPLES, 250).

-record(dps, {timestamp, counter}).

%% ===================================================================
%% API
%% ===================================================================

send(Args) ->
    Dps = proplists:get_value(dps, Args),
    FileList = proplists:get_all_values(file, Args),
    DirList = proplists:get_all_values(directory, Args),
    TarList = proplists:get_all_values(tarball, Args),
    Timeout = proplists:get_value(timeout, Args),
    Queues = proplists:get_value(queues, Args, []),
    send(TarList, DirList, FileList, Dps, Timeout, Queues).

start_link(Args) ->
    Pid = spawn_link(?MODULE, send, [Args]),
    {ok, Pid}.

%% ===================================================================
%% Private
%% ===================================================================

wait_for_server(Try, MaxTries) when Try >= MaxTries ->
    io:format(".timeout~n");

wait_for_server(Try, MaxTries) ->
    io:format("."),
    Result = rmq_publish_server:wait_for_confirms(?TIMEOUT_INTERVAL),
    case Result of
        ok ->
            io:format("ready~n");
        timeout ->
            wait_for_server(Try + 1, MaxTries);
        waiting_for_acks ->
            timer:sleep(?TIMEOUT_INTERVAL),
            wait_for_server(Try + 1, MaxTries);
        nacks_received ->
            error_logger:error_report("NACKs received"),
            error(nacks_received)
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
    case H of
        {_Name, Buffer} -> rmq_publish_server:send(Buffer);
        _ -> send_file(H)
    end,
    send_files(T).

tick() ->
    receive
        tick -> ok
    end,
    erlang:send_after(?INTERVAL, self(), tick).

calculate_dps([]) ->
    0.0;

calculate_dps(Dps) ->
    Last = lists:nth(1, Dps),
    First = lists:last(Dps),
    CounterDiff = Last#dps.counter - First#dps.counter,
    TimeDiff = timer:now_diff(Last#dps.timestamp, First#dps.timestamp),
    case TimeDiff of
        0 -> 0.0;
        _ -> CounterDiff / (TimeDiff / 1000000.0)
    end.

show_info(Dps, Counter) ->
    CurrentDps = calculate_dps(Dps),
    io:format("published ~p files, DPS ~.1f       \r", [Counter, CurrentDps]).

update_dps(Dps, Counter) ->
    Len = length(Dps),
    Sample = #dps{timestamp = now(), counter = Counter},
    case length(Dps) of
        N when N >= ?MAX_DPS_SAMPLES ->
            [Sample|lists:sublist(Dps, 1, Len - 1)];
        _ ->
            [Sample|Dps]
    end.

allowed_to_send([]) ->
    true;

allowed_to_send([{Queue, MaxSize}|Rest]) ->
    case rmq_publish_server:get_queue_size(Queue) of
        N when N >= MaxSize -> false;
        _ -> allowed_to_send(Rest)
    end.

loop(Files, Dps, Counter, _Acc, _Max, _Q) when Counter >= length(Files) ->
    show_info(Dps, Counter);

loop(Files, Dps, Counter, Acc, MaxDps, Queues) ->
    NewDps = update_dps(Dps, Counter),
    show_info(NewDps, Counter),
    tick(),
    case allowed_to_send(Queues) of
        false ->
            loop(Files, NewDps, Counter, Acc, MaxDps, Queues);
        true ->
            LastSample = lists:nth(1, NewDps),
            Diff = timer:now_diff(now(), LastSample#dps.timestamp),
            F = Acc + (MaxDps * Diff) / 1000000.0,
            M = round(F),
            N = if
                    Counter + M > length(Files) -> length(Files) - Counter;
                    Counter + M =< length(Files) -> M
                end,
            send_files(lists:sublist(Files, Counter + 1, N)),
            loop(Files, NewDps, Counter + N, F - M, MaxDps, Queues)
    end.

loop(Files, MaxDps, Queues) ->
    Dps = #dps{timestamp = now(), counter = 0},
    erlang:send_after(?INTERVAL, self(), tick),
    loop(Files, [Dps], 0, 0.0, MaxDps, Queues).

get_filelist([], List) ->
    List;

get_filelist([H|T], List) ->
    Result = file:list_dir(H),
    NewList = case Result of
        {ok, Filenames} ->
            [filename:join(H, F) || F <- Filenames];
        {error, Reason} ->
            error_logger:error_report(["error accessing", H, Reason]),
            error(io_error)
    end,
    get_filelist(T, List ++ NewList).

get_filelist(Dirs) ->
    get_filelist(Dirs, []).

get_tarball_filelist([], List) ->
    List;

get_tarball_filelist([H|T], List) ->
    {ok, Files} = erl_tar:extract(H, [memory, compressed]),
    get_tarball_filelist(T, Files ++ List).

get_tarball_filelist(Tarballs) ->
    get_tarball_filelist(Tarballs, []).

send(Tarballs, Dirs, Files, MaxDps, Timeout, Queues) ->
    DirList = get_filelist(Dirs),
    TarList = get_tarball_filelist(Tarballs),
    loop(TarList ++ Files ++ DirList, MaxDps, Queues),
    wait_for_server(Timeout),
    application:stop(rmq_publish).
