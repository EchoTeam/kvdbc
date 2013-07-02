%%% vim: ts=4 sts=4 sw=4 expandtab

-module(kvdbc_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {module, kvdbc_cfg} = load_config(),
    kvdbc_sup:start_link().

stop(_State) ->
    ok.
    
load_config() ->
    {ok, BInstances} = application:get_env(kvdbc, backend_instances),
    Spec = config_mod_spec(BInstances),
    mod_gen:go(Spec).

config_mod_spec(Config) ->
    Vals = lists:umerge([
        [
            lists:flatten(io_lib:format("backend_val(~s, ~s) -> ~p", [I, K, V]))
            || {K, V} <- Opts
        ]
        || {I, Opts} <- Config
    ]),
    [
        ["-module(kvdbc_cfg).\n"],
        ["-export([backend_val/2]).\n"],
        [string:join(Vals, ";\n"), "."]
    ].


%% ===================================================================
%% Tests
%% ===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

config_mod_spec_test_() ->
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
            ResultSpec = config_mod_spec(Instances),
            ExpectedSpec = 
                <<"-module(kvdbc_cfg).\n"
                "-export([backend_val/2]).\n"
                "backend_val(instance1, callback_module) -> module1;\n"
                "backend_val(instance1, process_name) -> process1;\n"
                "backend_val(instance1, config) -> [{k1,v1}];\n"
                "backend_val(instance2, callback_module) -> module2;\n"
                "backend_val(instance2, process_name) -> process2;\n"
                "backend_val(instance2, config) -> [{k2,v2}].">>,

            ?debugFmt("ExpectedSpec:~n~s", [ExpectedSpec]),
            ?debugFmt("ResultSpec:~n~s", [ResultSpec]),

            ?assertEqual(ExpectedSpec, iolist_to_binary(ResultSpec)),
            ?assertEqual({module, kvdbc_cfg}, mod_gen:go(ResultSpec)),
            ?assertEqual(module1,
                kvdbc_cfg:backend_val(instance1, callback_module))
        end
    ].

-endif.
