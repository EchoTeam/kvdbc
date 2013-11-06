%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Instances = kvdbc_cfg:backends(),
    {ok, { {one_for_one, 5, 10}, instance_specs(Instances)} }.

instance_specs(Instances) ->
    lists:map(fun({Name, Options}) ->
        [Module, ProcessName] = [proplists:get_value(K, Options) ||
            K <- [callback_module, process_name]],
        {ProcessName, { Module, start_link, [Name, ProcessName] },
            permanent, 10000, worker, [Module]}
    end, Instances).




%% ===================================================================
%% Tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

instance_specs_test_() ->
    [
        fun() ->
            Instances = [
              {instance1, [
                {callback_module, module1},
                {process_name, process1},
                {config, [
                  {k1, v1}
                ]}
              ]},
              {instance2, [
                {callback_module, module2},
                {process_name, process2},
                {config, [
                  {k2, v2}
                ]}
              ]}
            ],
            Result = instance_specs(Instances),
            Expected = [
                {process1, {module1, start_link, [instance1, process1]},
                    permanent, 10000, worker, [module1]},
                {process2, {module2, start_link, [instance2, process2]},
                    permanent, 10000, worker, [module2]}
            ],
            ?assertEqual(Expected, Result)
        end
    ].

-endif.
