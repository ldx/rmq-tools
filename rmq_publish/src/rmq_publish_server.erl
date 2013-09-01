-module(rmq_publish_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([start_link/0, start_link/1, send/1, ready/0]).

-record(state, {channel, connection, exchange, key, last_sent, last_acked,
                headers}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

send(Message) ->
    gen_server:cast(?MODULE, {send, Message}).

ready() ->
    gen_server:call(?MODULE, {ready}).

init(Args) ->
    process_flag(trap_exit, true),
    Uri = proplists:get_value(uri, Args),
    Exchange = proplists:get_value(exchange, Args),
    Key = proplists:get_value(routing_key, Args),
    Headers = proplists:get_value(headers, Args),
    {ok, Params} = amqp_uri:parse(Uri),
    {ok, Connection} = amqp_connection:start(Params),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    monitor(process, Channel),
    ok = amqp_channel:register_return_handler(Channel, self()),
    ok = amqp_channel:register_confirm_handler(Channel, self()),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    {ok, #state{channel = Channel, connection = Connection,
                exchange = Exchange, key = Key, last_sent = 0, last_acked = 0,
                headers = Headers}}.

handle_info(#'basic.ack'{delivery_tag = Tag, multiple = _Multiple}, State) ->
    NewState = State#state{last_acked = Tag},
    {noreply, NewState};

handle_info(#'basic.nack'{delivery_tag = Tag, multiple = Multiple}, State) ->
    io:format("error: got nack for ~p (multiple: ~p)~n", [Tag, Multiple]),
    {stop, stopped, State};

handle_info({#'basic.return'{reply_text = <<"unroutable">>, exchange = _},
             Content}, State) ->
    io:format("error: message is unroutable ~p~n", [Content]),
    {stop, stopped, State};

handle_info({'DOWN', _Ref, process, Channel, Info}, State)
        when Channel =:= State#state.channel ->
    error_logger:error_report(["Channel died", Info]),
    {stop, Info, State};

handle_info(_Info, State) ->
    {noreply, State}.

handle_call({ready}, _From, State) ->
    LastSent = State#state.last_sent,
    LastAcked = State#state.last_acked,
    case LastSent of
        LastAcked -> {reply, ok, State};
        _ -> {reply, not_ok, State}
    end;

handle_call(Message, _From, State) ->
    {stop, Message, State}.

handle_cast({send, Payload}, State) ->
    Channel = State#state.channel,
    Key = State#state.key,
    X = State#state.exchange,
    MessageId = State#state.last_sent + 1,
    Publish = #'basic.publish'{ticket = MessageId, exchange = X,
                               routing_key = Key},
    Msg = #amqp_msg{props = #'P_basic'{headers = State#state.headers},
                    payload = Payload},
    amqp_channel:cast(Channel, Publish, Msg),
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
