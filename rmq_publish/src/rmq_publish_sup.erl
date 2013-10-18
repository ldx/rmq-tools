-module(rmq_publish_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, [Args]}, permanent,
                               60000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    start_link([]).

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Args).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
    RmqServer = ?CHILD(rmq_publish_server, worker, Args),
    Sender = ?CHILD(sender, worker, Args),
    {ok, {{one_for_one, 5, 10}, [RmqServer, Sender]}}.
