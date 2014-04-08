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
        Module = proplists:get_value(callback_module, Options),
        {sup_child_name(Name), { Module, start_link, [Name] },
        permanent, 10000, worker, [Module]}
    end, Instances).

sup_child_name(InstanceName) ->
    list_to_atom("kvdbc_" ++ atom_to_list(InstanceName) ++ "_supchild_id").


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
                {config, [
                  {k1, v1}
                ]}
              ]},
              {instance2, [
                {callback_module, module2},
                {config, [
                  {k2, v2}
                ]}
              ]}
            ],
            Result = instance_specs(Instances),
            Expected = [
                {kvdbc_instance1_supchild_id, {module1, start_link, [instance1]},
                    permanent, 10000, worker, [module1]},
                {kvdbc_instance2_supchild_id, {module2, start_link, [instance2]},
                    permanent, 10000, worker, [module2]}
            ],
            ?assertEqual(Expected, Result)
        end
    ].

-endif.
