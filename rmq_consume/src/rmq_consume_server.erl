-module(rmq_consume_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, start_link/1]).

-record(state, {directory, channel, tag, connection, n, timer, timeout,
                verbosity}).

%% ===================================================================
%% API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%% ===================================================================
%% Callbacks
%% ===================================================================

init(Args) ->
    process_flag(trap_exit, true),
    Directory = proplists:get_value(directory, Args),
    Timeout = proplists:get_value(timeout, Args) * 1000,
    Verbosity = proplists:get_value(verbose, Args, 0),
    {ok, Connection} = amqp_connection:start(get_amqp_params(Args)),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    monitor(process, Channel),
    Sub = #'basic.consume'{queue = get_queue(Args)},
    #'basic.consume_ok'{consumer_tag = Tag} =
        amqp_channel:subscribe(Channel, Sub, self()),
    Timer = update_timer(no_timer, Timeout),
    {ok, #state{directory = Directory, channel = Channel, tag = Tag,
                connection = Connection, n = 0, timer = Timer,
                timeout = Timeout, verbosity = Verbosity}}.

handle_info({timeout}, State) ->
    spawn(fun() -> application:stop(rmq_consume) end),
    {noreply, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{} = Info, Content}, State) ->
    MTag = Info#'basic.deliver'.delivery_tag,
    #'amqp_msg'{props = _, payload = Payload} = Content,
    {ok, Filename} = save_file(State#state.directory, MTag, Payload),
    amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = MTag}),
    N = State#state.n + 1,
    log_progress(State#state.verbosity, N, Filename, Info),
    Timer = update_timer(State#state.timer, State#state.timeout),
    {noreply, State#state{n = N, timer = Timer}};

handle_info({'DOWN', _Ref, process, Channel, Info}, State)
        when Channel =:= State#state.channel ->
    error_logger:error_report(["Channel died", Info]),
    {stop, Info, State};

handle_info(Info, State) ->
    {stop, Info, State}.

handle_call(Message, _From, State) ->
    {stop, Message, State}.

handle_cast(Message, State) ->
    {stop, Message, State}.

terminate(Reason, State) ->
    io:format("~nterminating: ~p~n", [Reason]),
    close(State#state.connection, State#state.channel, State#state.tag),
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% Private
%% ===================================================================

get_amqp_params(Args) ->
    Prop = proplists:get_value(uri, Args),
    {ok, Param} = amqp_uri:parse(Prop),
    Param.

get_queue(Args) ->
    Queue = proplists:get_value(queue, Args),
    case Queue of
        undefined -> error(no_queue_specified);
        _ -> list_to_binary(Queue)
    end.

save_file(Dir, Tag, Suffix, Content) ->
    Name = integer_to_list(Tag),
    Name1 = case Suffix of
                0 -> Name;
                N -> lists:concat([Name, "_", N - 1])
            end,
    Filename = filename:join(Dir, Name1),
    case file:open(Filename, [write, exclusive]) of
        {ok, File} ->
            ok = file:write(File, Content),
            ok = file:close(File),
            {ok, Filename};
        {error, eexist} ->
            save_file(Dir, Tag, Suffix + 1, Content);
        {error, Reason} ->
            Error = io_lib:format("error creating ~p: ~p", [Name1, Reason]),
            {error, Error}
    end.

save_file(Dir, Tag, Content) ->
    save_file(Dir, Tag, 0, Content).

update_timer(OldTimer, Timeout) ->
    if Timeout == 0 ->
           no_timer;
       OldTimer =:= no_timer ->
           erlang:send_after(Timeout, self(), {timeout});
       Timeout /= 0, OldTimer =/= no_timer ->
           erlang:cancel_timer(OldTimer),
           erlang:send_after(Timeout, self(), {timeout})
    end.

close(Connection, Channel, CTag) ->
    #'basic.cancel_ok'{} =
        amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = CTag}),
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).

log_progress(Verbosity, N, Filename, MsgProps) ->
    case Verbosity of
        X when X >= 1, is_integer(X) ->
            io:format("~p <- ~p~n", [Filename, MsgProps]);
        _ ->
            io:format("consumed ~B messages\r", [N])
    end.
