-module(mc_conn_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-export([start_connection/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{mc_connection, {mc_connection, start_link, [mc_daemon]},
            temporary, brutal_kill, worker, [mc_connection]}]}}.

start_connection(NS) ->
    {ok, Pid} = supervisor:start_child(?MODULE, [NS]),
    gen_tcp:controlling_process(NS, Pid),
    %% Tell this mc_connection it's the controlling process and ready to go.
    Pid ! go,
    {ok, Pid}.
