-module(mc_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(DbName, JsonMode) ->
    application:start(sasl),
    supervisor:start_link({local, ?SERVER}, ?MODULE, [DbName, JsonMode]).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([DbName, JsonMode]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    Daemon = {mc_daemon, {mc_daemon, start_link, [DbName, JsonMode]},
              Restart, Shutdown, Type, [mc_daemon]},

    TcpListener = {mc_tcp_listener, {mc_tcp_listener, start_link, [11213, mc_daemon]},
                   Restart, Shutdown, Type, [mc_tcp_listener, mc_daemon]},

    ConnSup = {mc_conn_sup, {mc_conn_sup, start_link, []},
               Restart, Shutdown, supervisor, dynamic},

    {ok, {SupFlags, [Daemon, TcpListener, ConnSup]}}.
