-module(rmq_publish_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, start_link/1, send/1, wait_for_confirms/1,
         get_queue_size/1]).

-record(state, {channel, connection, exchange, key, last_sent, last_acked,
                headers, immediate, mandatory}).

%% ===================================================================
%% API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

send(Message) ->
    gen_server:cast(?MODULE, {send, Message}).

wait_for_confirms(Timeout) ->
    gen_server:call(?MODULE, {wait_for_confirms, Timeout}).

get_queue_size(Queue) when is_list(Queue) ->
    get_queue_size(list_to_binary(Queue));

get_queue_size(Queue) when is_binary(Queue) ->
    gen_server:call(?MODULE, {queue_size, Queue}).

%% ===================================================================
%% Callbacks
%% ===================================================================

init(Args) ->
    process_flag(trap_exit, true),
    Uri = proplists:get_value(uri, Args),
    Exchange = proplists:get_value(exchange, Args),
    Key = proplists:get_value(routing_key, Args),
    Headers = proplists:get_value(headers, Args),
    Immediate = proplists:get_value(immediate, Args, false),
    Mandatory = proplists:get_value(mandatory, Args, false),
    {ok, Params} = amqp_uri:parse(Uri),
    {ok, Connection} = amqp_connection:start(Params),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    monitor(process, Channel),
    ok = amqp_channel:register_return_handler(Channel, self()),
    ok = amqp_channel:register_confirm_handler(Channel, self()),
    ok = amqp_channel:register_flow_handler(Channel, self()),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    {ok, #state{channel = Channel, connection = Connection,
                exchange = Exchange, key = Key, last_sent = 0, last_acked = 0,
                headers = Headers, immediate = Immediate,
                mandatory = Mandatory}}.

handle_info(#'basic.ack'{delivery_tag = Tag, multiple = _Multiple}, State) ->
    NewState = State#state{last_acked = Tag},
    {noreply, NewState};

handle_info(#'basic.nack'{delivery_tag = Tag, multiple = Multiple}, State) ->
    error_logger:error_report(["got NACK", Tag, Multiple]),
    {stop, stopped, State};

handle_info({#'channel.flow'{active = Active}}, State) ->
    case Active of
        true ->
            error_logger:info_report(["broker started flow control"]);
        false ->
            error_logger:info_report(["broker stopped flow control"])
    end,
    {noreply, State};

handle_info({#'basic.return'{reply_text = <<"unroutable">>, exchange = _},
             Content}, State) ->
    error_logger:error_report(["message is unroutable", Content]),
    {stop, stopped, State};

handle_info({'DOWN', _Ref, process, Channel, Info}, State)
        when Channel =:= State#state.channel ->
    error_logger:error_report(["Channel died", Info]),
    {stop, Info, State};

handle_info(Info, State) ->
    {stop, Info, State}.

handle_call({wait_for_confirms, Timeout}, _From, State) ->
    LastSent = State#state.last_sent,
    LastAcked = State#state.last_acked,
    case LastSent of
        LastAcked ->
            Result = amqp_channel:wait_for_confirms(State#state.channel,
                                                    Timeout),
            case Result of
                timeout ->
                    {reply, timeout, State};
                false ->
                    {reply, nacks_received, State};
                true ->
                    {reply, ok, State}
            end;
        _ ->
            {reply, waiting_for_acks, State}
    end;

handle_call({queue_size, Q}, _From, State) ->
    #'queue.declare_ok'{queue = Q, message_count = Cnt} =
    amqp_channel:call(State#state.channel,
                      #'queue.declare'{queue = Q, passive = true}),
    {reply, Cnt, State};

handle_call(Message, _From, State) ->
    {stop, Message, State}.

handle_cast({send, Payload}, State) ->
    MessageId = State#state.last_sent + 1,
    Publish = #'basic.publish'{ticket = MessageId,
                               exchange = State#state.exchange,
                               routing_key = State#state.key,
                               immediate = State#state.immediate,
                               mandatory = State#state.mandatory},
    Msg = #amqp_msg{props = #'P_basic'{headers = State#state.headers},
                    payload = Payload},
    ok = amqp_channel:cast(State#state.channel, Publish, Msg),
    NewState = State#state{last_sent = MessageId},
    {noreply, NewState};

handle_cast(Message, State) ->
    {stop, Message, State}.

terminate(Reason, State) ->
    io:format("terminating: ~p~n", [Reason]),
    ok = amqp_channel:close(State#state.channel),
    ok = amqp_connection:close(State#state.connection),
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.
